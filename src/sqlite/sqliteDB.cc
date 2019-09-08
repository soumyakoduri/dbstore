// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sqliteDB.h"

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
	string schema;
	struct RGWOpParams t_params = {};

	if (!*sdb) {
		printf("In SQLInsertUser - no db\n");
		goto out;
	}

	t_params.user_table = params->user_table;
	t_params.user_name = ":user";

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateInsertUser failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLInsertUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":user");

	if (index <=0)  {
	        printf("Failed to fetch ':user' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->user_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->user_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}

	return 0;
}

int SQLInsertUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = createUserTable(params);

		if (ret)
			return ret;

		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateInsertBucket failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLInsertBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":user");

	if (index <=0)  {
	        printf("Failed to fetch ':user' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->user_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->user_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}

	index = sqlite3_bind_parameter_index(stmt, ":bucket");

	if (index <=0)  {
	        printf("Failed to fetch ':bucket' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->bucket_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->bucket_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}
	return 0;
}

int SQLInsertBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = createBucketTable(params);

		if (ret)
			return ret;

		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateInsertObject failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLInsertObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":object");

	if (index <=0)  {
	        printf("Failed to fetch ':object' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->object.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for object(%s) index (%d) with error(%s) \n",
			params->object.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}

	index = sqlite3_bind_parameter_index(stmt, ":bucket");

	if (index <=0)  {
	        printf("Failed to fetch ':bucket' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->bucket_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->bucket_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}
	return 0;
}

int SQLInsertObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = createObjectTable(params);

		if (ret)
			return ret;

		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateRemoveUser failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLRemoveUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":user");

	if (index <=0)  {
	        printf("Failed to fetch ':user' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->user_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->user_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}

	return 0;
}

int SQLRemoveUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateRemoveBucket failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLRemoveBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":bucket");

	if (index <=0)  {
	        printf("Failed to fetch ':bucket' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->bucket_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for bucket(%s) index (%d) with error(%s) \n",
			params->bucket_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}
	return 0;
}

int SQLRemoveBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateRemoveObject failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLRemoveObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":object");

	if (index <=0)  {
	        printf("Failed to fetch ':object' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->object.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for object(%s) index (%d) with error(%s) \n",
			params->object.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}

	index = sqlite3_bind_parameter_index(stmt, ":bucket");

	if (index <=0)  {
	        printf("Failed to fetch ':bucket' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->bucket_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->bucket_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}
	return 0;
}

int SQLRemoveObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateListUser failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLListUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":user");

	if (index <=0)  {
	        printf("Failed to fetch ':user' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->user_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for user(%s) index (%d) with error(%s) \n",
			params->user_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}

	return 0;
}

int SQLListUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	if (!ret) {
                printf("%d, %s \n", sqlite3_column_int(stmt, 0),
			         	sqlite3_column_text(stmt, 1));
	}

	Reset(stmt);

	if (ret)
		goto out;
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

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateListBucket failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLListBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":bucket");

	if (index <=0)  {
	        printf("Failed to fetch ':bucket' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->bucket_name.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for bucket(%s) index (%d) with error(%s) \n",
			params->bucket_name.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}
	return 0;
}

int SQLListBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	if (!ret) {
                printf("%d, %s, %s \n", sqlite3_column_int(stmt, 0),
			         	sqlite3_column_text(stmt, 1),
			       	        sqlite3_column_text(stmt, 2));
	}

	Reset(stmt);

	if (ret)
		goto out;
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
	t_params.object = ":object";

	schema = Schema(&t_params);

	sqlite3_prepare_v2 (*sdb, schema.c_str(), -1, &stmt , NULL);

	if (!stmt)
	        printf("PrepateListObject failed (%s) \n", sqlite3_errmsg(*sdb));

out:
	return ret;
}

int SQLListObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;

	index = sqlite3_bind_parameter_index(stmt, ":object");

	if (index <=0)  {
	        printf("Failed to fetch ':object' index (%d), error(%s) \n",
			index, sqlite3_errmsg(*sdb));
		return -1;
	}

	rc = sqlite3_bind_text(stmt, index, params->object.c_str(), -1, SQLITE_STATIC);

	if (rc != SQLITE_OK) {
	        printf("sqlite bind text failed for object(%s) index (%d) with error(%s) \n",
			params->object.c_str(), index, sqlite3_errmsg(*sdb));
		return -1;
	}
	return 0;
}

int SQLListObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	if (!stmt) {
		ret = Prepare(params);
	}

	if (!stmt) {
		return ret;
	}

	ret = Bind(params);
	if (ret)
		goto out;

	ret = Step(stmt);

	if (!ret) {
                printf("%d, %s, %s \n", sqlite3_column_int(stmt, 0),
			         	sqlite3_column_text(stmt, 1),
			       	        sqlite3_column_text(stmt, 2));
	}

	Reset(stmt);

	if (ret)
		goto out;
out:
	return ret;
}
