package service

import (
	"fmt"
	"log"
	"strings"

	"canal-adapter/config"
	"canal-adapter/pkg/client"
	pb "canal-adapter/protocol"

	dsnUtil "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type RdbMirrorService struct {
	connector     *client.SimpleCanalConnector
	db            *gorm.DB
	isDebugEnable bool
}

func NewRdbMirrorService(db *gorm.DB, address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32, isDebugEnable bool) *RdbMirrorService {
	svc := new(RdbMirrorService)
	svc.connector = client.NewSimpleCanalConnector(config.Env.Canal.Address, config.Env.Canal.Port, config.Env.Canal.UserName, config.Env.Canal.Password, config.Env.Canal.Destination, config.Env.Canal.SoTimeOut, config.Env.Canal.IdleTimeOut)
	svc.isDebugEnable = isDebugEnable
	svc.db = db
	return svc
}

func (rdb *RdbMirrorService) GetConnector() *client.SimpleCanalConnector {
	return rdb.connector
}

func (rdb *RdbMirrorService) Connect() error {
	err := rdb.GetConnector().Connect()
	if err != nil {
		return err
	}
	return nil
}

func (rdb *RdbMirrorService) DisConnection() error {
	return rdb.GetConnector().DisConnection()
}

func (rdb *RdbMirrorService) Subscribe(filter string) error {
	return rdb.GetConnector().Subscribe(filter)
}

func (rdb *RdbMirrorService) UnSubscribe() error {
	return rdb.GetConnector().UnSubscribe()
}

func (rdb *RdbMirrorService) Get(batchSize int32, timeOut *int64, units *int32) (*pb.Message, error) {
	return rdb.GetConnector().Get(batchSize, timeOut, units)
}

func (rdb *RdbMirrorService) GetWithoutAck(batchSize int32, timeOut *int64, units *int32) (*pb.Message, error) {
	return rdb.GetConnector().GetWithOutAck(batchSize, timeOut, units)
}

func (rdb *RdbMirrorService) Ack(batchId int64) error {
	return rdb.GetConnector().Ack(batchId)
}

func (rdb *RdbMirrorService) RollBack(batchId int64) error {
	return rdb.GetConnector().RollBack(batchId)
}

func (rdb *RdbMirrorService) IsDebugEnable() bool {
	return rdb.isDebugEnable
}

func (rdb *RdbMirrorService) Mirror(entries []pb.Entry) {
	for _, entry := range entries {
		if entry.GetEntryType() == pb.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pb.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pb.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err != nil {
			log.Printf("Unmarshal rowChange fail...")
			continue
		}

		if getIsDdl(rowChange.GetEventType()) {
			rdb.MirrorDdl(entry, rowChange)
		} else {
			rdb.MirrorDml(entry, rowChange)
		}
	}
}

func (rdb *RdbMirrorService) MirrorDdl(entry pb.Entry, rowChange *pb.RowChange) {
	sql := rowChange.GetSql()
	sourceDb := rowChange.GetDdlSchemaName()
	destDb := getDbNameFromGormDB(rdb.db)
	sql = strings.Replace(sql, sourceDb, destDb, -1)
	if len(sql) > 0 {
		rdb.db.Exec(sql)
	}
}

func (rdb *RdbMirrorService) MirrorDml(entry pb.Entry, rowChange *pb.RowChange) {
	eventType := rowChange.GetEventType()
	header := entry.GetHeader()
	tableName := header.GetTableName()
	if rdb.isDebugEnable {
		log.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
	}
	for _, rowData := range rowChange.GetRowDatas() {
		if eventType == pb.EventType_DELETE {
			rdb.remove(tableName, rowData)
		} else if eventType == pb.EventType_INSERT {
			rdb.insert(tableName, rowData)
		} else if eventType == pb.EventType_UPDATE {
			rdb.update(tableName, rowData)
		}
	}
}

func (rdb *RdbMirrorService) insert(tableName string, data *pb.RowData) {
	columns := data.GetAfterColumns()
	record := make(map[string]interface{})
	for _, col := range columns {
		// 处理canal把datetime和json的Null解析成空字符串""
		if isExtraType(col.GetMysqlType()) && col.GetValue() == "" {
			continue
		}
		record[col.GetName()] = col.GetValue()
	}
	rdb.db.Table(tableName).Create(record)
}

func (rdb *RdbMirrorService) update(tableName string, data *pb.RowData) {
	columns := data.GetAfterColumns()
	record := make(map[string]interface{})
	var primaryKeyName string
	var primaryKeyValue interface{}
	for _, col := range columns {
		// 处理canal把datetime和json的Null解析成空字符串""
		if isExtraType(col.GetMysqlType()) && col.GetValue() == "" {
			continue
		}
		if col.GetUpdated() {
			record[col.GetName()] = col.GetValue()
		}

		if col.GetIsKey() {
			primaryKeyName = col.GetName()
			primaryKeyValue = col.GetValue()
		}
	}
	where := fmt.Sprintf("%s = ?", primaryKeyName)
	rdb.db.Table(tableName).Where(where, primaryKeyValue).Updates(record)
}

func (rdb *RdbMirrorService) remove(tableName string, data *pb.RowData) {
	columns := data.GetBeforeColumns()
	var primaryKeyName string
	var primaryKeyValue interface{}
	for _, col := range columns {
		if col.GetIsKey() {
			primaryKeyName = col.GetName()
			primaryKeyValue = col.GetValue()
		}
	}
	sql := fmt.Sprintf("delete from %s where %s = ?", tableName, primaryKeyName)
	rdb.db.Exec(sql, primaryKeyValue)
}

func getIsDdl(eventType pb.EventType) bool {
	if eventType == pb.EventType_INSERT || eventType == pb.EventType_UPDATE || eventType == pb.EventType_DELETE {
		return false
	}
	return true
}

func getDbNameFromGormDB(db *gorm.DB) string {
	dia := db.Config.Dialector
	if dialector, ok := dia.(*driver.Dialector); ok {
		dsn := dialector.Config.DSN
		url, err := dsnUtil.ParseDSN(dsn)
		if err != nil {
			return ""
		}
		return url.DBName
	}

	return ""
}

func isExtraType(typeName string) bool {
	return strings.Contains(typeName, "datetime") || strings.Contains(typeName, "json")
}
