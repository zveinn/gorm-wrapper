package gormwrapper

import (
	"log"
	"os"
	"time"

	gorm "github.com/jinzhu/gorm"
	uuid "github.com/satori/go.uuid"
	"github.com/zkynetio/logger"
	L "github.com/zkynetio/logger"
)

func Migrate(tag string, m interface{}, drop bool) {
	if drop {
		ConnectionMap[tag].DropTableIfExists(m)
	}
	ConnectionMap[tag].AutoMigrate(m)
}

type BaseModel struct {
	CreatedAt *time.Time `json:"-"`
	UpdatedAt *time.Time `json:"-"`
	DeletedAt *time.Time `json:"-" sql:"index"`
	ID        uuid.UUID  `json:"id,omitepmty" gorm:"primary_key;type:uuid;column:id"`
}

func (m *BaseModel) BeforeCreate(scope *gorm.Scope) error {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return scope.SetColumn("ID", uid)
}

func Create(tag string, m interface{}) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Create(m).Error)
}

func CreateFromMap(tag string, m interface{}, mMap map[string]interface{}) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Model(m).Create(mMap).Error)
}

func KeyValueGet(tag string, m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Where(key+" = ?", value).First(m).Error)
}

func MultiKeyValueGet(tag string, m interface{}, key, value []string) *L.InformationConstruct {
	q := ConnectionMap[tag]
	for i := 0; i < len(key)-1; i++ {
		for ii := 0; ii < len(value)-1; ii++ {
			q.Where(key[i]+" = ?", value[ii])
		}
	}
	return L.ParsePGError(q.First(m).Error)
}

func KeyValueGetList(tag string, m interface{}, key, value string, limit, offset int) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Where(key+" = ?", value).Limit(limit).Offset(offset).Find(m).Error)
}

func KeyValueSelectGetList(tag string, m interface{}, selKey, key, value string, limit, offset int) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Select(selKey).Where(key+" = ?", value).Limit(limit).Offset(offset).Find(m).Error)
}

func GetList(tag string, m interface{}, limit, offset int) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Limit(limit).Offset(offset).Find(m).Error)
}

func KeyValueGetWithRelations(tag string, m interface{}, key, value string, relations []string, autoload bool) *L.InformationConstruct {
	dbcon := ConnectionMap[tag].Set("gorm:auto_preload", autoload)
	for _, relation := range relations {
		dbcon = dbcon.Preload(relation)
	}
	return L.ParsePGError(dbcon.First(m, key+" = ?", value).Error)
}

func KeyValueUpdate(tag string, m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Model(m).Where(key+" = ?", value).Updates(m).Error)
}

func KeyValueUpdateOrCreate(tag string, m interface{}, key, value string) *L.InformationConstruct {
	query := ConnectionMap[tag].Model(m).Where(key+" = ?", value).Updates(m)
	if query.Error != nil {
		return L.ParsePGError(query.Error)
	} else if query.RowsAffected == 0 {
		return L.ParsePGError(ConnectionMap[tag].Create(m).Error)
	}

	return nil

}

func KeyValueDelete(tag string, m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Where(key+" = ?", value).Delete(m).Error)
}

func KeyValueHardDelete(tag string, m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Unscoped().Where(key+" = ?", value).Delete(m).Error)
}

func Increment(tag string, m interface{}, key string) error {
	return L.ParsePGError(ConnectionMap[tag].Model(m).Set(key, " = "+key+" + 1").Updates(m).Error)
}

func KeyValueUpdateColumn(tag string, m interface{}, filterKey, value, key string, newValue interface{}) error {
	return L.ParsePGError(ConnectionMap[tag].Model(m).Where(filterKey+" = ?", value).Update(key, newValue).Error)
}

func KeyValueWhereInSelect(tag string, m interface{}, key, value, selKey, inKey string, inList interface{}) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Select(selKey).Where(key+" = ?", value).Where(inKey+" IN (?)", inList).Find(m).Error)
}

func KeyValueWhereIn(tag string, m interface{}, key, value, inKey string, inList interface{}) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Where(key+" = ?", value).Where(inKey+" IN (?)", inList).Find(m).Error)
}

func WhereIn(tag string, m interface{}, inKey string, inList interface{}) *L.InformationConstruct {
	return L.ParsePGError(ConnectionMap[tag].Where(inKey+" IN (?)", inList).Find(m).Error)
}

var ConnectionMap = make(map[string]*gorm.DB)

func CloseDB(tag string) {
	ConnectionMap[tag].Close()
}

func Ping(tag string) error {
	return ConnectionMap[tag].DB().Ping()
}

func Connect(dialect string, connectionString string, tag string) *logger.InformationConstruct {
	Connection, err := gorm.Open(dialect, connectionString)
	if err != nil {
		xErr := logger.DatabaseConnectionErrror(err)
		xErr.Log()
		return xErr
	}
	ConnectionMap[tag] = Connection
	return nil
}

func DeleteDatabaseFile(file string) error {
	return os.Remove(file)
}

func DropPostgresDatabase(tag string, dbname string) {
	if _, err := ConnectionMap[tag].DB().Exec("DROP DATABASE " + dbname); err != nil {
		panic(err)
	}
}

func CreateDatabase(tag string, dbname string, owner string) {
	if _, err := ConnectionMap[tag].DB().Exec("CREATE DATABASE " + dbname + " WITH OWNER " + owner); err != nil {
		panic(err)
	}
}

func SetLoggerFile(tag string, filepath string) {
	var file, err1 = os.Create(filepath)
	if err1 != nil {
		panic(err1)
	}
	ConnectionMap[tag].SetLogger(log.New(file, "", log.LstdFlags|log.Lshortfile))
}

func SetMaxIdleConns(tag string, count int) {
	ConnectionMap[tag].DB().SetMaxIdleConns(count)
}

func SetMaxOpenConns(tag string, count int) {
	ConnectionMap[tag].DB().SetMaxOpenConns(count)
}

func SetConnMaxLifetime(tag string, duration time.Duration) {
	ConnectionMap[tag].DB().SetConnMaxLifetime(duration)
}
