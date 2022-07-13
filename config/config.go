package config

import "os"

var GiakhoPostgresHost = os.Getenv("GIAKHO_POSTGRES_HOST")
var GiakhoPostgresPort = os.Getenv("GIAKHO_POSTGRES_PORT")
var GiakhoPostgresUser = os.Getenv("GIAKHO_POSTGRES_USER")
var GiakhoPostgresPassword = os.Getenv("GIAKHO_POSTGRES_PASSWORD")
var GiakhoPostgresDb = os.Getenv("GIAKHO_POSTGRES_DB")

var BqProjectId = os.Getenv("BQ_PROJECT_ID")
var BqWebsyncDataset = os.Getenv("BQ_WEBSYNC_DATASET")
var BqPresyncDataset = os.Getenv("BQ_PRESYNC_DATASET")

var StreamingBatchSize = os.Getenv("STREAMING_BATCH_SIZE")
var StreamingDbTables = os.Getenv("STREAMING_DB_TABLES")
var Separator = ","

var KiotVietClientID = os.Getenv("KIOTVIET_CLIENT_ID")
var KiotVietClientSecret = os.Getenv("KIOTVIET_CLIENT_SECRET")
var KiotVietRetailer = os.Getenv("KIOTVIET_RETAILER")
var KiotVietUserName = os.Getenv("KIOTVIET_USERNAME")
var KiotVietPassWord = os.Getenv("KIOTVIET_PASSWORD")
