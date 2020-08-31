package gormwrapper

import (
	"log"
	"os"
	"time"

	gorm "github.com/jinzhu/gorm"
	uuid "github.com/satori/go.uuid"
	L "github.com/zkynetio/logger"
)

func Migrate(m interface{}, drop bool) {
	if drop {
		Connection.DropTableIfExists(m)
	}
	Connection.AutoMigrate(m)
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

func Create(m interface{}) *L.InformationConstruct {
	return L.ParsePGError(Connection.Create(m).Error)
}

func CreateFromMap(m interface{}, mMap map[string]interface{}) *L.InformationConstruct {
	return L.ParsePGError(Connection.Model(m).Create(mMap).Error)
}

func KeyValueGet(m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(Connection.Where(key+" = ?", value).First(m).Error)
}

func MultiKeyValueGet(m interface{}, key, value []string) *L.InformationConstruct {
	q := Connection
	for i := 0; i < len(key)-1; i++ {
		for ii := 0; ii < len(value)-1; ii++ {
			q.Where(key[i]+" = ?", value[ii])
		}
	}
	return L.ParsePGError(q.First(m).Error)
}

func KeyValueGetList(m interface{}, key, value string, limit, offset int) *L.InformationConstruct {
	return L.ParsePGError(Connection.Where(key+" = ?", value).Limit(limit).Offset(offset).Find(m).Error)
}

func KeyValueSelectGetList(m interface{}, selKey, key, value string, limit, offset int) *L.InformationConstruct {
	return L.ParsePGError(Connection.Select(selKey).Where(key+" = ?", value).Limit(limit).Offset(offset).Find(m).Error)
}

func GetList(m interface{}, limit, offset int) *L.InformationConstruct {
	return L.ParsePGError(Connection.Limit(limit).Offset(offset).Find(m).Error)
}

func KeyValueGetWithRelations(m interface{}, key, value string, relations []string, autoload bool) *L.InformationConstruct {
	dbcon := Connection.Set("gorm:auto_preload", autoload)
	for _, relation := range relations {
		dbcon = dbcon.Preload(relation)
	}
	return L.ParsePGError(dbcon.First(m, key+" = ?", value).Error)
}

func KeyValueUpdate(m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(Connection.Model(m).Where(key+" = ?", value).Updates(m).Error)
}

func KeyValueUpdateOrCreate(m interface{}, key, value string) *L.InformationConstruct {
	query := Connection.Model(m).Where(key+" = ?", value).Updates(m)
	if query.Error != nil {
		return L.ParsePGError(query.Error)
	} else if query.RowsAffected == 0 {
		return L.ParsePGError(Connection.Create(m).Error)
	}

	return nil

}

func KeyValueDelete(m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(Connection.Where(key+" = ?", value).Delete(m).Error)
}

func KeyValueHardDelete(m interface{}, key, value string) *L.InformationConstruct {
	return L.ParsePGError(Connection.Unscoped().Where(key+" = ?", value).Delete(m).Error)
}

func Increment(m interface{}, key string) error {
	return L.ParsePGError(Connection.Model(m).Set(key, " = "+key+" + 1").Updates(m).Error)
}

func KeyValueUpdateColumn(m interface{}, filterKey, value, key string, newValue interface{}) error {
	return L.ParsePGError(Connection.Model(m).Where(filterKey+" = ?", value).Update(key, newValue).Error)
}

func KeyValueWhereInSelect(m interface{}, key, value, selKey, inKey string, inList interface{}) *L.InformationConstruct {
	return L.ParsePGError(Connection.Select(selKey).Where(key+" = ?", value).Where(inKey+" IN (?)", inList).Find(m).Error)
}

func KeyValueWhereIn(m interface{}, key, value, inKey string, inList interface{}) *L.InformationConstruct {
	return L.ParsePGError(Connection.Where(key+" = ?", value).Where(inKey+" IN (?)", inList).Find(m).Error)
}

func WhereIn(m interface{}, inKey string, inList interface{}) *L.InformationConstruct {
	return L.ParsePGError(Connection.Where(inKey+" IN (?)", inList).Find(m).Error)
}

var Connection *gorm.DB
var ConnectionMap = make(map[string]*gorm.DB)

func CloseDB() {
	Connection.Close()
}

func Ping() error {
	return Connection.DB().Ping()
}

func Connect(dialect string, connectionString string) {
	var err error
	Connection, err = gorm.Open(dialect, connectionString)
	if err != nil {
		panic(err)
	}
	ConnectionMap["default"] = Connection

}

func ConnectOther(dialect string, connectionString string, tag string) {
	var err error
	var conn *gorm.DB
	conn, err = gorm.Open(dialect, connectionString)
	if err != nil {
		panic(err)
	}
	ConnectionMap[tag] = conn
}

func DeleteDatabaseFile(file string) error {
	return os.Remove(file)
}

func DropPostgresDatabase(dbname string) {
	if _, err := Connection.DB().Exec("DROP DATABASE " + dbname); err != nil {
		panic(err)
	}
}

func CreateDatabase(dbname string, owner string) {
	if _, err := Connection.DB().Exec("CREATE DATABASE " + dbname + " WITH OWNER " + owner); err != nil {
		panic(err)
	}
}

func SetLoggerFile(filepath string) {
	var file, err1 = os.Create(filepath)
	if err1 != nil {
		panic(err1)
	}
	Connection.SetLogger(log.New(file, "", log.LstdFlags|log.Lshortfile))
}

func SetMaxIdleConns(count int) {
	Connection.DB().SetMaxIdleConns(count)
}

func SetMaxOpenConns(count int) {
	Connection.DB().SetMaxOpenConns(count)
}

func SetConnMaxLifetime(duration time.Duration) {
	Connection.DB().SetConnMaxLifetime(duration)
}
