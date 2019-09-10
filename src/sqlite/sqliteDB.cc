// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sqliteDB.h"

#define SQL_PREPARE(params, sdb, stmt, ret, Op) \
do {						\
	string schema;			   	\
	schema = Schema(&params);	   	\
					   	\
	sqlite3_prepare_v2 (*sdb, schema.c_str(), \
		            -1, &stmt , NULL);	\
	if (!stmt) {				\
	        printf("%s failed (%s) \n",	\
			Op, sqlite3_errmsg(*sdb));\
		ret = -1;			\
		goto out;			\
	}					\
	ret = 0;				\
} while(0);

#define SQL_BIND_INDEX(stmt, index, str, sdb)	\
do {						\
	index = sqlite3_bind_parameter_index(stmt, str);     \
							     \
	if (index <=0)  {				     \
	        printf("Failed to fetch %s index (%d), error(%s) \n", \
			str, index, sqlite3_errmsg(*sdb));   \
		rc = -1;				     \
		goto out;				     \
	}						     \
}while(0);

#define SQL_BIND_TEXT(stmt, index, str, sdb)			\
do {								\
	rc = sqlite3_bind_text(stmt, index, str, -1, SQLITE_STATIC); \
								\
	if (rc != SQLITE_OK) {					\
	        printf("sqlite bind text failed for user(%s)"	\
			"index (%d) with error(%s) \n",		\
			str, index, sqlite3_errmsg(*sdb));	\
		rc = -1;					\
		goto out;					\
	}							\
}while(0);

#define SQL_EXECUTE(params, stmt)		\
do{						\
	if (!stmt) {				\
		ret = Prepare(params);		\
	}					\
						\
	if (!stmt) {				\
		goto out;			\
	}					\
						\
	ret = Bind(params);			\
	if (ret)				\
		goto out;			\
						\
	ret = Step(stmt);			\
						\
	Reset(stmt);				\
						\
	if (ret)				\
		goto out;			\
}while(0);

static int list_callback(void *None, int argc, char **argv, char **aname)
{
        int i;
        for(i=0; i<argc; i++) {
                printf("%s = %s \n", aname[i], argv[i]? argv[i] : "NULL");
        }
        return 0;
}

void *SQLiteDB::openDB()
{
	string dbname;
        int rc = 0;
	char *errmsg = NULL;

	dbname	= getDBname();
	if (dbname.empty())
		goto out;

	rc = sqlite3_open_v2(dbname.c_str(), &db,
			     SQLITE_OPEN_READWRITE |
			     SQLITE_OPEN_CREATE |
			     SQLITE_OPEN_FULLMUTEX,
			     NULL);

        if (rc) {
	        fprintf(stderr, "Cant open %s: %s\n",
			dbname.c_str(),
                        sqlite3_errmsg(db));
        } else {
                fprintf(stderr, "Opened database(%s) successfully\n",
			dbname.c_str());
        }

	exec("PRAGMA foreign_keys=ON", NULL);
out:
	return db;
}

int SQLiteDB::closeDB()
{
	if (db)
		sqlite3_close((sqlite3 *)db);

	db = NULL;
	return 0;
}

int SQLiteDB::Reset(sqlite3_stmt *stmt)
{
	int ret = -1;

	if (!stmt) {
		return -1;
	}
	sqlite3_clear_bindings(stmt);
	ret = sqlite3_reset(stmt);
out:
	return ret;
}

int SQLiteDB::Step(sqlite3_stmt *stmt)
{
	int ret = -1;

	if (!stmt)
		return -1;

	ret = sqlite3_step(stmt);

	if ((ret != SQLITE_DONE) && (ret != SQLITE_ROW)) {
	        printf("sqlite step failed (%s) \n", sqlite3_errmsg(db));
		return -1;
	}

	return 0;
}

int SQLiteDB::exec(const char *schema,
	       int (*callback)(void*,int,char**,char**))
{
	int ret = -1;
	char *errmsg = NULL;

	if (!db)
		goto out;

	ret = sqlite3_exec(db, schema, callback, 0, &errmsg);
        if (ret != SQLITE_OK) {
                fprintf(stderr, "SQL execution failed of schema(%s) %s\n", schema, errmsg);
                sqlite3_free(errmsg);
		goto out;
        }
	ret = 0;
out:
	return ret;
}

int SQLiteDB::createTables()
{
	int ret = -1;
	int cu, cb, co = -1;
	RGWOpParams params = {};

	params.user_table = getUserTable();
	params.bucket_table = getBucketTable();
	params.object_table = getObjectTable();

	if (cu = createUserTable(&params))
		goto out;

	if (cb = createBucketTable(&params))
		goto out;

	if (co = createObjectTable(&params))
		goto out;

	ret = 0;
out:
	if (ret) {
		if (cu)
			DeleteUserTable(&params);
		if (cb)
			DeleteBucketTable(&params);
		if (co)
			DeleteObjectTable(&params);
	}

	return ret;
}

int SQLiteDB::createUserTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("User", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		cout<<"CreateUserTable failed \n";

out:
	return ret;
}

int SQLiteDB::createBucketTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Bucket", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		cout<<"CreateBucketTable failed \n";

out:
	return ret;
}

int SQLiteDB::createObjectTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Object", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		cout<<"CreateObjectTable failed \n";

out:
	return ret;
}

int SQLiteDB::DeleteUserTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->user_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		cout<<"DeleteUserTable failed \n";

out:
	return ret;
}

int SQLiteDB::DeleteBucketTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->bucket_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		cout<<"DeletebucketTable failed \n";

out:
	return ret;
}

int SQLiteDB::DeleteObjectTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->object_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		cout<<"DeleteObjectTable failed \n";

out:
	return ret;
}

int SQLiteDB::ListAllUsers(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->user_table);
	cout<<"########### Listing all Users #############\n";
	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		cout<<"ListUsertable failed \n";

out:
	return ret;
}

int SQLiteDB::ListAllBuckets(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->bucket_table);

	cout<<"########### Listing all Buckets #############\n";
	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		cout<<"Listbuckettable failed \n";

out:
	return ret;
}

int SQLiteDB::ListAllObjects(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->object_table);
	cout<<"########### Listing all Objects #############\n";

	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		cout<<"ListObjecttable failed \n";

out:
	return ret;
}

int SQLInsertUser::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLInsertUser - no db\n");
		goto out;
	}

	t_params.user_table = params->user_table;
	t_params.user_name = ":user";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareInsertUser");
out:
	return ret;
}

int SQLInsertUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":user", sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLRemoveUser::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLRemoveUser - no db\n");
		goto out;
	}

	t_params.user_table = params->user_table;
	t_params.user_name = ":user";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareRemoveUser");
out:
	return ret;
}

int SQLRemoveUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":user", sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLListUser::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLListUser - no db\n");
		goto out;
	}

	t_params.user_table = params->user_table;
	t_params.user_name = ":user";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareListUser");

out:
	return ret;
}

int SQLListUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":user", sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

out:
	return rc;
}

int SQLListUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLInsertBucket::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLInsertBucket - no db\n");
		goto out;
	}

	t_params.bucket_table = params->bucket_table;
	t_params.user_name = ":user";
	t_params.bucket_name = ":bucket";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareInsertBucket");

out:
	return ret;
}

int SQLInsertBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":user", sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, ":bucket", sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLRemoveBucket::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpParams t_params = {};
	string schema;

	if (!*sdb) {
		printf("In SQLRemoveBucket - no db\n");
		goto out;
	}

	t_params.bucket_table = params->bucket_table;
	t_params.bucket_name = ":bucket";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareRemoveBucket");

out:
	return ret;
}

int SQLRemoveBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":bucket", sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLListBucket::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLListBucket - no db\n");
		goto out;
	}

	t_params.bucket_table = params->bucket_table;
	t_params.bucket_name = ":bucket";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareListBucket");

out:
	return ret;
}

int SQLListBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":bucket", sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLListBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLInsertObject::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLInsertObject - no db\n");
		goto out;
	}

	t_params.object_table = params->object_table;
	t_params.bucket_name = ":bucket";
	t_params.object = ":object";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareInsertObject");

out:
	return ret;
}

int SQLInsertObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":object", sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, ":bucket", sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLRemoveObject::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLRemoveObject - no db\n");
		goto out;
	}

	t_params.object_table = params->object_table;
	t_params.bucket_name = ":bucket";
	t_params.object = ":object";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareRemoveObject");

out:
	return ret;
}

int SQLRemoveObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":object", sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, ":bucket", sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}

int SQLListObject::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLListObject - no db\n");
		goto out;
	}

	t_params.object_table = params->object_table;
	t_params.bucket_name = ":bucket";
	t_params.object = ":object";

	SQL_PREPARE(t_params, sdb, stmt, ret,
		    "PrepareListObject");

out:
	return ret;
}

int SQLListObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	SQL_BIND_INDEX(stmt, index, ":object", sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, ":bucket", sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLListObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt);
out:
	return ret;
}
