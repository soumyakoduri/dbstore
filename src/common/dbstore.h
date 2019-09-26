// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DB_STORE_H
#define DB_STORE_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include <fmt/format.h>

using namespace std;
class DBstore;

struct RGWOpParams {
	string tenant;
	string user_table;
	string bucket_table;
	string object_table;
	string objectdata_table;
	string user_name;
	string bucket_name;
	string object;
	size_t offset;
	string data;
	size_t datalen;
};

/* Used for prepared schemas.
 * Difference with above structure is that all 
 * the fields are strings here to accommodate any
 * style identifiers used by backend db
 */
struct RGWOpPrepareParams {
	string tenant;
	string user_table;
	string bucket_table;
	string object_table;
	string objectdata_table;
	string user_name;
	string bucket_name;
	string object;
	string offset;
	string data;
	string datalen;
};

struct SchemaParams {
	bool is_prepare;
	union {
		struct RGWOpParams *params;
		struct RGWOpPrepareParams *p_params;
	} u;
};

struct RGWOps {
	class InsertUserOp *InsertUser;
	class RemoveUserOp *RemoveUser;
	class ListUserOp *ListUser;
	class InsertBucketOp *InsertBucket;
	class RemoveBucketOp *RemoveBucket;
	class ListBucketOp *ListBucket;
	class InsertObjectOp *InsertObject;
	class RemoveObjectOp *RemoveObject;
	class ListObjectOp *ListObject;
	class PutObjectDataOp *PutObjectData;
	class GetObjectDataOp *GetObjectData;
	class DeleteObjectDataOp *DeleteObjectData;
};

class RGWOp {
	private:
	const string CreateUserTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' (	\
			UserName TEXT PRIMARY KEY NOT NULL UNIQUE \n);";
	const string CreateBucketTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' ( \
			BucketName TEXT PRIMARY KEY NOT NULL UNIQUE , \
			UserName TEXT NOT NULL, \
			FOREIGN KEY (UserName) \
				REFERENCES '{}' (UserName) ON DELETE CASCADE ON UPDATE CASCADE \n);";
	const string CreateObjectTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' ( \
			BucketName TEXT NOT NULL , \
			ObjectName TEXT NOT NULL , \
			PRIMARY KEY (BucketName, ObjectName), \
			FOREIGN KEY (BucketName) \
				REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";
        const string CreateObjectDataTableQ =
                "CREATE TABLE IF NOT EXISTS '{}' ( \
			BucketName TEXT NOT NULL , \
			ObjectName TEXT NOT NULL , \
                        Offset   INTEGER NOT NULL, \
                        Data     BLOB,             \
			Size 	 INTEGER NOT NULL, \
			PRIMARY KEY (BucketName, ObjectName, Offset), \
                        FOREIGN KEY (BucketName, ObjectName) \
                                REFERENCES '{}' (BucketName, ObjectName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

	const string DropQ = "DROP TABLE IF EXISTS '{}'";
	const string ListAllQ = "SELECT  * from '{}'";

	public:
	RGWOp() {};

	string CreateTableSchema(string type, RGWOpParams *params);
	string DeleteTableSchema(string table_name);
	string ListTableSchema(string table_name);

	virtual int Prepare(RGWOpParams *params) { return 0; }
	virtual int Execute(RGWOpParams *params) { return 0; }
};

class InsertUserOp : public RGWOp {
	private:
	const string Query = "INSERT INTO '{}' (UserName) VALUES ({});";

	public:
	string Schema(SchemaParams *s_params);
};

class RemoveUserOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where UserName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class ListUserOp: public RGWOp {
	private:
	const string Query =
	"SELECT  * from '{}' where UserName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class InsertBucketOp: public RGWOp {
	private:
	const string Query =
	"INSERT INTO '{}' (BucketName, UserName) VALUES ({}, {})";

	public:
	string Schema(SchemaParams *s_params);
};

class RemoveBucketOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class ListBucketOp: public RGWOp {
	private:
	const string Query =
	"SELECT  * from '{}' where BucketName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class InsertObjectOp: public RGWOp {
	private:
	const string Query =
	"INSERT INTO '{}' (BucketName, ObjectName) VALUES ({}, {})";

	public:
	string Schema(SchemaParams *s_params);
};

class RemoveObjectOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {} and ObjectName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class ListObjectOp: public RGWOp {
	private:
	const string Query =
	"SELECT  * from '{}' where BucketName = {} and ObjectName = {}";
	// XXX: Include queries for specific bucket and user too

	public:
	string Schema(SchemaParams *s_params);
};

class PutObjectDataOp: public RGWOp {
	private:
	const string Query =
	"INSERT INTO '{}' (BucketName, ObjectName, Offset, Data, Size) \
       		VALUES ({}, {}, {}, {}, {})";

	public:
	string Schema(SchemaParams *s_params);
};

class GetObjectDataOp: public RGWOp {
	private:
	const string Query =
	"SELECT * from '{}' where BucketName = {} and ObjectName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class DeleteObjectDataOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {} and ObjectName = {}";

	public:
	string Schema(SchemaParams *s_params);
};

class DBstore {
	private:
	const string tenant;
	const string user_table;
	const string bucket_table;
	const string object_table; // XXX: Make it per bucket but then it shall not
				   // be straightforward to save prepared statement.
	const string objectdata_table; // XXX: Make it per bucket

	public:	
	DBstore(string tenant_name) : tenant(tenant_name),
       				user_table(tenant_name+".user.table"),
			        bucket_table(tenant_name+".bucket.table"),
				object_table(tenant_name+".object.table"),
				objectdata_table(tenant_name+".objectdata.table")
       			        {}

	string getDBname();
	string getUserTable();
	string getBucketTable();
	string getObjectTable();
	string getObjectDataTable();

	struct RGWOps rgwops; // RGW operations, make it private?
	void *db; // Backend database handle, make it private?

	int InitializeParams(string Op, RGWOpParams *params);
	int ProcessOp(string Op, RGWOpParams *params);
	RGWOp* getRGWOp(string Op);

        virtual void *openDB() { return NULL; }
        virtual int closeDB() { return 0; }
	virtual int createTables() { return 0; }

        virtual int ListAllBuckets(RGWOpParams *params) = 0;
        virtual int ListAllUsers(RGWOpParams *params) = 0;
        virtual int ListAllObjects(RGWOpParams *params) = 0;

};
#endif
