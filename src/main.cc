#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <dbstore.h>

#ifdef SQLITE_ENABLED
#include <sqliteDB.h>
#endif

void initialize_RGWOps (string tenant, class DBstore *db)
{
#ifdef SQLITE_ENABLED
	(void)db->createTables();
	db->rgwops.InsertUser = new SQLInsertUser(tenant, db);
	db->rgwops.RemoveUser = new SQLRemoveUser(tenant, db);
	db->rgwops.ListUser = new SQLListUser(tenant, db);
	db->rgwops.InsertBucket = new SQLInsertBucket(tenant, db);
	db->rgwops.RemoveBucket = new SQLRemoveBucket(tenant, db);
	db->rgwops.ListBucket = new SQLListBucket(tenant, db);
	db->rgwops.InsertObject = new SQLInsertObject(tenant, db);
	db->rgwops.RemoveObject = new SQLRemoveObject(tenant, db);
	db->rgwops.ListObject = new SQLListObject(tenant, db);
#else
	db->rgwops.InsertUser = NULL;
#endif
}

int main(int argc, char *argv[])
{
	string tenant = "Redhat";

	string user1 = "Soumya";
	string bucketa = "rgw";
	string objecta1 = "bugfixing";
	string objecta2 = "zipper";
	string bucketb = "gluster";
	string objectb1 = "bugfixing";
	string objectb2 = "delegations";

	string user2 = "Kasturi";
	string bucketc = "qe";
	string objectc1 = "rhhi";
	string objectc2 = "cns";

	struct RGWOpParams params = {};

	class DBstore *db;
	int rc = 0;

#ifdef SQLITE_ENABLED
	db = new SQLiteDB(tenant);
#else
	db = new DBstore(tenant);
#endif

	db->db = db->openDB();
	initialize_RGWOps(tenant, db);

	db->InitializeParams("InsertUser", &params);
	params.user_name = user1;
	db->ProcessOp("InsertUser", &params);

	params.bucket_name = bucketa;
	db->ProcessOp("InsertBucket", &params);

	params.object = objecta1;
	db->ProcessOp("InsertObject", &params);
	params.object = objecta2;
	db->ProcessOp("InsertObject", &params);

	params.bucket_name = bucketb;
	db->ProcessOp("InsertBucket", &params);

	params.object = objectb1;
	db->ProcessOp("InsertObject", &params);
	params.object = objectb2;
	db->ProcessOp("InsertObject", &params);

	params.user_name = user2;
	db->ProcessOp("InsertUser", &params);

	params.bucket_name = bucketc;
	db->ProcessOp("InsertBucket", &params);

	params.object = objectc1;
	db->ProcessOp("InsertObject", &params);
	params.object = objectc2;
	db->ProcessOp("InsertObject", &params);

	db->ProcessOp("ListUser", &params);
	db->ProcessOp("ListBucket", &params);
	db->ProcessOp("ListObject", &params);

	db->ListAllUsers(&params);
	db->ListAllBuckets(&params);
	db->ListAllObjects(&params);

	params.object = objecta1;
	params.bucket_name = bucketa;
	db->ProcessOp("RemoveObject", &params);

	params.bucket_name = bucketb;
	db->ProcessOp("RemoveBucket", &params);

	params.user_name = user2;
	db->ProcessOp("RemoveUser", &params);

	db->ListAllUsers(&params);
	db->ListAllBuckets(&params);
	db->ListAllObjects(&params);

	db->closeDB();

	return 0;
}
