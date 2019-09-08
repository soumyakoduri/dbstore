// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore.h"

using namespace std;

string DBstore::getDBname() {
	return tenant + ".db";
}

string RGWOp::CreateTableSchema(string type, RGWOpParams *params) {
	if (!type.compare("User"))
		return fmt::format(CreateUserTableQ.c_str(),
			           params->user_table.c_str());
	if (!type.compare("Bucket"))
		return fmt::format(CreateBucketTableQ.c_str(),
			           params->bucket_table.c_str(),
				   params->user_table.c_str());
	if (!type.compare("Object"))
		return fmt::format(CreateObjectTableQ.c_str(),
			           params->object_table.c_str(),
				   params->bucket_table.c_str());

	return NULL;
}

string RGWOp::DeleteTableSchema(string table) {
	return fmt::format(DropQ.c_str(), table.c_str());
}

string RGWOp::ListTableSchema(string table) {
	return fmt::format(ListAllQ.c_str(), table.c_str());
}

string InsertUserOp::Schema(RGWOpParams *params) {
	string user_table, user_name;

	if (!params)
		return NULL;

	user_table = params->user_table;
	user_name = params->user_name;

	return fmt::format(Query.c_str(),
		 params->user_table.c_str(), user_name.c_str());
}

string InsertBucketOp::Schema(RGWOpParams *params) {
	string bucket_table, bucket, user;

	if (!params)
		return NULL;

	bucket_table = params->bucket_table;
	bucket = params->bucket_name;
	user = params->user_name;

	return fmt::format(Query.c_str(),
		 bucket_table.c_str(), bucket.c_str(), user.c_str());
}

string InsertObjectOp::Schema(RGWOpParams *params) {
	string object_table, object, bucket_name;

	if (!params)
		return NULL;

	object_table = params->object_table;
	object = params->object;
	bucket_name = params->bucket_name;

	return fmt::format(Query.c_str(),
		 object_table.c_str(), bucket_name.c_str(), object.c_str());
}

string RemoveUserOp::Schema(RGWOpParams *params) {

	string user_table, user;

	if (!params)
		return NULL;

	user_table = params->user_table;
	user = params->user_name;

	return fmt::format(Query.c_str(),
		 user_table.c_str(), user.c_str());
}

string RemoveBucketOp::Schema(RGWOpParams *params) {
	string bucket_table, bucket;

	if (!params)
		return NULL;

	bucket_table = params->bucket_table;
	bucket = params->bucket_name;

	return fmt::format(Query.c_str(), bucket_table.c_str(), bucket.c_str());
}

string RemoveObjectOp::Schema(RGWOpParams *params) {
	string object_table, object, bucket;

	if (!params)
		return NULL;

	object_table = params->object_table;
	object = params->object;
	bucket = params->bucket_name;

	return fmt::format(Query.c_str(), object_table.c_str(), bucket.c_str(), object.c_str());
}

string ListUserOp::Schema(RGWOpParams *params) {

	string user_table, user;

	if (!params)
		return NULL;

	user_table = params->user_table;
	user = params->user_name;

	return fmt::format(Query.c_str(),
		 user_table.c_str(), user.c_str());
}

string ListBucketOp::Schema(RGWOpParams *params) {
	string bucket_table, bucket;

	if (!params)
		return NULL;

	bucket_table = params->bucket_table;
	bucket = params->bucket_name;

	return fmt::format(Query.c_str(), bucket_table.c_str(), bucket.c_str());
}

string ListObjectOp::Schema(RGWOpParams *params) {
	string object_table, object;

	if (!params)
		return NULL;

	object_table = params->object_table;
	object = params->object;

	return fmt::format(Query.c_str(), object_table.c_str(), object.c_str());
}

RGWOp * DBstore::getRGWOp(string Op)
{
	if (!Op.compare("InsertUser"))
		return rgwops.InsertUser;
	if (!Op.compare("InsertBucket"))
		return rgwops.InsertBucket;
	if (!Op.compare("InsertObject"))
		return rgwops.InsertObject;
	if (!Op.compare("RemoveUser"))
		return rgwops.RemoveUser;
	if (!Op.compare("RemoveBucket"))
		return rgwops.RemoveBucket;
	if (!Op.compare("RemoveObject"))
		return rgwops.RemoveObject;
	if (!Op.compare("ListUser"))
		return rgwops.ListUser;
	if (!Op.compare("ListBucket"))
		return rgwops.ListBucket;
	if (!Op.compare("ListObject"))
		return rgwops.ListObject;

	return NULL;
}

int DBstore::InitializeParams(string Op, RGWOpParams *params)
{
	int ret = -1;

	if (!params)
		goto out;

	//reset params here
	params->tenant = tenant;
	params->user_table = user_table;
	params->bucket_table = bucket_table;
	params->object_table = object_table;

out:
	return ret;
}

int DBstore::ProcessOp(string Op, struct RGWOpParams *params) {
	int ret = -1;
	class RGWOp *rgw_op;

	rgw_op = getRGWOp(Op);

	ret = rgw_op->Execute(params);

	if (ret)
		cout<<"In Process op Execute failed for fop : "<<Op.c_str()<<" \n";
//	else
//		cout<<"In Process op Execute succeeded for fop : "<<Op.c_str()<<" \n";

	return ret;
}
