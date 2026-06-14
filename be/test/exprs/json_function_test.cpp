// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>
#include <vector>

#include "common/exception.h"
#include "exprs/json_functions.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

// mock
class JsonFunctionTest : public testing::Test {
public:
    JsonFunctionTest() {}
};

static std::vector<JsonPath> parse_paths(const std::string& path) {
    std::vector<JsonPath> vec;
    JsonFunctions::parse_json_paths(path, &vec);
    return vec;
}

TEST_F(JsonFunctionTest, json_path1) {
    bool wrap_explicitly;
    std::string json_raw_data(
            "[{\"k1\":\"v1\",\"keyname\":{\"ip\":\"10.10.0.1\",\"value\":20}},{\"k1\":\"v1-1\","
            "\"keyname\":{\"ip\":\"10.20.10.1\",\"value\":20}}]");
    rapidjson::Document jsonDoc;
    if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
        EXPECT_TRUE(false);
    }
    rapidjson::Value* res3;
    res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].keyname.ip", &jsonDoc,
                                                          jsonDoc.GetAllocator(), &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());

    res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].k1", &jsonDoc,
                                                          jsonDoc.GetAllocator(), &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());

    res3 = JsonFunctions::get_json_array_from_parsed_json("$", &jsonDoc, jsonDoc.GetAllocator(),
                                                          &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());
    rapidjson::StringBuffer buffer;
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    (*res3)[0].Accept(writer);
    EXPECT_EQ(json_raw_data, std::string(buffer.GetString()));
}

TEST_F(JsonFunctionTest, json_path_get_nullobject) {
    bool wrap_explicitly;
    std::string json_raw_data(
            "[{\"a\":\"a1\", \"b\":\"b1\", \"c\":\"c1\"},{\"a\":\"a2\", "
            "\"c\":\"c2\"},{\"a\":\"a3\", \"b\":\"b3\", \"c\":\"c3\"}]");
    rapidjson::Document jsonDoc;
    if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
        EXPECT_TRUE(false);
    }

    rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
            "$.[*].b", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());
    EXPECT_EQ(res3->Size(), 3);
}

TEST_F(JsonFunctionTest, json_path_test) {
    bool wrap_explicitly;
    {
        std::string json_raw_data("[{\"a\":\"a1\", \"b\":\"b1\"}, {\"a\":\"a2\", \"b\":\"b2\"}]");
        rapidjson::Document jsonDoc;
        if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
            EXPECT_TRUE(false);
        }

        rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
                "$.[*].a", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res3->IsArray());
        EXPECT_EQ(res3->Size(), 2);
    }
    {
        std::string json_raw_data(
                "{\"a\":[\"a1\",\"a2\"], \"b\":[\"b1\",\"b2\"], \"c\":[\"c1\"], \"d\":[], "
                "\"e\":\"e1\"}");
        rapidjson::Document jsonDoc;
        if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
            EXPECT_TRUE(false);
        }

        rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
                "$.a", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res3->IsArray());
        EXPECT_EQ(res3->Size(), 2);

        rapidjson::Value* res4 = JsonFunctions::get_json_array_from_parsed_json(
                "$.c", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res4->IsArray());
        EXPECT_EQ(res4->Size(), 1);
        EXPECT_FALSE(wrap_explicitly);

        rapidjson::Value* res5 = JsonFunctions::get_json_array_from_parsed_json(
                "$.d", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res5->IsArray());
        EXPECT_EQ(res5->Size(), 0);
        EXPECT_FALSE(wrap_explicitly);

        rapidjson::Value* res6 = JsonFunctions::get_json_array_from_parsed_json(
                "$.e", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res6->IsArray());
        EXPECT_EQ(res6->Size(), 1);
        EXPECT_TRUE(wrap_explicitly);
    }
}

TEST_F(JsonFunctionTest, json_path_to_string) {
    // invalid path -> "INVALID"
    EXPECT_EQ(JsonPath("anything", 3, false).to_string(), "INVALID");
    // key only
    EXPECT_EQ(JsonPath("abc", -1, true).to_string(), "abc");
    // key with wildcard index
    EXPECT_EQ(JsonPath("abc", -2, true).to_string(), "abc[*]");
    // key with numeric index
    EXPECT_EQ(JsonPath("abc", 5, true).to_string(), "abc[5]");
    // empty key with numeric index
    EXPECT_EQ(JsonPath("", 2, true).to_string(), "[2]");
    // empty key, no index
    EXPECT_EQ(JsonPath("", -1, true).to_string(), "");

    EXPECT_EQ(JsonPath("k", 7, true).debug_string(), "key:k, idx:7, valid:true");
    EXPECT_EQ(JsonPath("", -1, false).debug_string(), "key:, idx:-1, valid:false");
}

TEST_F(JsonFunctionTest, parse_json_paths_basic) {
    {
        // quoted key containing a dot should not be split.
        std::vector<JsonPath> vec = parse_paths("$.\"text.abc\".xyz");
        ASSERT_EQ(vec.size(), 3);
        EXPECT_EQ(vec[0].key, "$");
        EXPECT_TRUE(vec[0].is_valid);
        EXPECT_EQ(vec[1].key, "text.abc");
        EXPECT_EQ(vec[2].key, "xyz");
    }
    {
        // key with array index.
        std::vector<JsonPath> vec = parse_paths("$.a[3]");
        ASSERT_EQ(vec.size(), 2);
        EXPECT_EQ(vec[1].key, "a");
        EXPECT_EQ(vec[1].idx, 3);
        EXPECT_TRUE(vec[1].is_valid);
    }
    {
        // wildcard index.
        std::vector<JsonPath> vec = parse_paths("$.a[*]");
        ASSERT_EQ(vec.size(), 2);
        EXPECT_EQ(vec[1].idx, -2);
    }
    {
        // path not starting with '$' marks the first elem invalid.
        std::vector<JsonPath> vec = parse_paths("a.b");
        ASSERT_FALSE(vec.empty());
        EXPECT_FALSE(vec[0].is_valid);
    }
}

TEST_F(JsonFunctionTest, parse_json_paths_malformed_throws) {
    // A trailing escape character makes boost::escaped_list_separator throw,
    // which parse_json_paths converts into a doris::Exception.
    std::vector<JsonPath> vec;
    EXPECT_THROW(JsonFunctions::parse_json_paths("$.abc\\", &vec), doris::Exception);
}

TEST_F(JsonFunctionTest, get_json_object_root_and_member) {
    std::string json_raw_data("{\"a\": {\"b\": 1}, \"c\": 2}");
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json_raw_data.c_str()).HasParseError());

    // "$" returns the whole document.
    std::vector<JsonPath> root = parse_paths("$");
    rapidjson::Value* res =
            JsonFunctions::get_json_object_from_parsed_json(root, &doc, doc.GetAllocator());
    ASSERT_TRUE(res != nullptr);
    EXPECT_TRUE(res->IsObject());

    // nested object member.
    std::vector<JsonPath> p = parse_paths("$.a.b");
    res = JsonFunctions::get_json_object_from_parsed_json(p, &doc, doc.GetAllocator());
    ASSERT_TRUE(res != nullptr);
    EXPECT_TRUE(res->IsInt());
    EXPECT_EQ(res->GetInt(), 1);

    // not existing member -> nullptr.
    std::vector<JsonPath> miss = parse_paths("$.x");
    res = JsonFunctions::get_json_object_from_parsed_json(miss, &doc, doc.GetAllocator());
    EXPECT_TRUE(res == nullptr);

    // invalid first path elem -> nullptr.
    std::vector<JsonPath> invalid = parse_paths("a");
    res = JsonFunctions::get_json_object_from_parsed_json(invalid, &doc, doc.GetAllocator());
    EXPECT_TRUE(res == nullptr);
}

TEST_F(JsonFunctionTest, get_json_object_array_index) {
    std::string json_raw_data("{\"arr\": [10, 20, 30]}");
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json_raw_data.c_str()).HasParseError());

    // valid index.
    std::vector<JsonPath> p = parse_paths("$.arr[1]");
    rapidjson::Value* res =
            JsonFunctions::get_json_object_from_parsed_json(p, &doc, doc.GetAllocator());
    ASSERT_TRUE(res != nullptr);
    EXPECT_EQ(res->GetInt(), 20);

    // index out of bounds -> nullptr.
    std::vector<JsonPath> oob = parse_paths("$.arr[5]");
    res = JsonFunctions::get_json_object_from_parsed_json(oob, &doc, doc.GetAllocator());
    EXPECT_TRUE(res == nullptr);

    // wildcard [*] on an array.
    std::vector<JsonPath> star = parse_paths("$.arr[*]");
    res = JsonFunctions::get_json_object_from_parsed_json(star, &doc, doc.GetAllocator());
    ASSERT_TRUE(res != nullptr);
    EXPECT_TRUE(res->IsArray());
    EXPECT_EQ(res->Size(), 3);
}

TEST_F(JsonFunctionTest, get_json_object_index_on_non_array) {
    std::string json_raw_data("{\"a\": 7}");
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json_raw_data.c_str()).HasParseError());

    // indexing a scalar value returns nullptr.
    std::vector<JsonPath> p = parse_paths("$.a[0]");
    rapidjson::Value* res =
            JsonFunctions::get_json_object_from_parsed_json(p, &doc, doc.GetAllocator());
    EXPECT_TRUE(res == nullptr);
}

TEST_F(JsonFunctionTest, get_json_object_array_of_objects) {
    // arr is an array whose elements exercise every branch in the array loop of
    // match_value: object with array member, object with scalar member, object
    // missing the member (insert null), null element, scalar element, and
    // nested-array element.
    std::string json_raw_data("{\"arr\": [{\"k\": [1, 2]}, {\"k\": 5}, {\"m\": 9}, null, 7, [8]]}");
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json_raw_data.c_str()).HasParseError());

    std::vector<JsonPath> p = parse_paths("$.arr.k");
    rapidjson::Value* res =
            JsonFunctions::get_json_object_from_parsed_json(p, &doc, doc.GetAllocator());
    ASSERT_TRUE(res != nullptr);
    EXPECT_TRUE(res->IsArray());
    // 1, 2 (flattened array), 5 (scalar), null (missing member insert).
    EXPECT_EQ(res->Size(), 4);
}

TEST_F(JsonFunctionTest, print_json_value) {
    rapidjson::Document doc;
    doc.Parse("{\"a\":1}");
    EXPECT_EQ(JsonFunctions::print_json_value(doc), "{\"a\":1}");

    rapidjson::Value scalar(123);
    EXPECT_EQ(JsonFunctions::print_json_value(scalar), "123");
}

TEST_F(JsonFunctionTest, merge_objects) {
    {
        // nested merge + add missing member.
        rapidjson::Document dst;
        dst.Parse("{\"a\": {\"c\": 1}}");
        rapidjson::Document src;
        src.Parse("{\"a\": {\"d\": 2}, \"e\": 3}");
        JsonFunctions::merge_objects(dst, src, dst.GetAllocator());
        ASSERT_TRUE(dst["a"].IsObject());
        EXPECT_TRUE(dst["a"].HasMember("c"));
        EXPECT_TRUE(dst["a"].HasMember("d"));
        EXPECT_TRUE(dst.HasMember("e"));
        EXPECT_EQ(dst["e"].GetInt(), 3);
    }
    {
        // existing null member is overwritten by the source value.
        rapidjson::Document dst;
        dst.Parse("{\"a\": null}");
        rapidjson::Document src;
        src.Parse("{\"a\": 5}");
        JsonFunctions::merge_objects(dst, src, dst.GetAllocator());
        ASSERT_TRUE(dst["a"].IsInt());
        EXPECT_EQ(dst["a"].GetInt(), 5);
    }
    {
        // existing non-null scalar member is kept (not overwritten).
        rapidjson::Document dst;
        dst.Parse("{\"a\": 1}");
        rapidjson::Document src;
        src.Parse("{\"a\": 2}");
        JsonFunctions::merge_objects(dst, src, dst.GetAllocator());
        EXPECT_EQ(dst["a"].GetInt(), 1);
    }
    {
        // non-object source is a no-op.
        rapidjson::Document dst;
        dst.Parse("{\"a\": 1}");
        rapidjson::Document src;
        src.Parse("[1, 2, 3]");
        JsonFunctions::merge_objects(dst, src, dst.GetAllocator());
        EXPECT_EQ(dst["a"].GetInt(), 1);
        EXPECT_FALSE(dst.HasMember("0"));
    }
}

TEST_F(JsonFunctionTest, is_root_path) {
    EXPECT_TRUE(JsonFunctions::is_root_path(parse_paths("$.")));
    EXPECT_FALSE(JsonFunctions::is_root_path(parse_paths("$")));
    EXPECT_FALSE(JsonFunctions::is_root_path(parse_paths("$.a")));
}

} // namespace doris
