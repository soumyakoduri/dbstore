// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef SQLITE_DB_H
#define SQLITE_DB_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <sqlite3.h>
#include <dbstore.h>

using namespace std;

class SQLiteDB : public DBstore, public RGWOp{
	private:
	sqlite3 *db = NULL; // make this private again

	public:	
	sqlite3_stmt *stmt = NULL;

	SQLiteDB(string tenant_name) : DBstore(tenant_name) {}
	SQLiteDB(string tenant_name, sqlite3 *dbi) : DBstore(tenant_name), db(dbi) {}
	~SQLiteDB() {
		printf("closing db (%p)\n", db);
		if(db)
			sqlite3_close(db);
		db = NULL;
	}
	int exec(const char *schema,
		 int (*callback)(void*,int,char**,char**));
	void *openDB();
	int closeDB();
	int Step(sqlite3_stmt *stmt, int (*cbk)(sqlite3_stmt *stmt));
	int Reset(sqlite3_stmt *stmt);

	int createTables();
	int createBucketTable(RGWOpParams *params);
	int createUserTable(RGWOpParams *params);
	int createObjectTable(RGWOpParams *params);
	int createObjectDataTable(RGWOpParams *params);

	int DeleteBucketTable(RGWOpParams *params);
	int DeleteUserTable(RGWOpParams *params);
	int DeleteObjectTable(RGWOpParams *params);
	int DeleteObjectDataTable(RGWOpParams *params);

	int ListAllBuckets(RGWOpParams *params);
	int ListAllUsers(RGWOpParams *params);
	int ListAllObjects(RGWOpParams *params);
};

class SQLInsertUser : public SQLiteDB, public InsertUserOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLInsertUser(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLRemoveUser : public SQLiteDB, public RemoveUserOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLRemoveUser(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLListUser : public SQLiteDB, public ListUserOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLListUser(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLInsertBucket : public SQLiteDB, public InsertBucketOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLInsertBucket(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLRemoveBucket : public SQLiteDB, public RemoveBucketOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLRemoveBucket(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLListBucket : public SQLiteDB, public ListBucketOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLListBucket(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLInsertObject : public SQLiteDB, public InsertObjectOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLInsertObject(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLRemoveObject : public SQLiteDB, public RemoveObjectOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLRemoveObject(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLListObject : public SQLiteDB, public ListObjectOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLListObject(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLPutObjectData : public SQLiteDB, public PutObjectDataOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLPutObjectData(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLGetObjectData : public SQLiteDB, public GetObjectDataOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLGetObjectData(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};

class SQLDeleteObjectData : public SQLiteDB, public DeleteObjectDataOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLDeleteObjectData(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
        int Prepare(RGWOpParams *params);
        int Execute(RGWOpParams *params);
        int Bind(RGWOpParams *params);
};
#endif
