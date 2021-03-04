#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <iostream>
using namespace std;
#include "db.hpp"

DB* db = nullptr;
FILE* key_file = fopen("./DB_key.log", "w");
FILE* err_file = fopen("./DB_err.log", "w");
FILE* right_file = fopen("./DB_ri.log", "w");

std::string _make_random_string(const int len) {
	const char* alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	const auto alphabet_len = strlen(alphabet);

	std::string result;

	for (int i = 0; i < len; ++i)
		result += alphabet[rand() % alphabet_len];

	return result;	
}

Slice _make_slice(std::string s) {
	char* buf = new char[s.size()];
	memcpy(buf, s.c_str(), s.size());
	return Slice(buf, s.size());
}

void test_correctness(const int count) {
	srand(time(NULL));

	puts("Correctness testing starts.");

	// Generate some actions
	std::vector<std::pair<std::string, std::string>> kvs;
	// Rewrite test with the same key
    string prefix("0123456789");
	kvs.emplace_back("1234567890123456", "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
	kvs.emplace_back("1234567890123456", "09876543210987654321098765432109876543210987654321098765432109876543210987654321");
	for (int i = 0; i < count; ++i)
		kvs.emplace_back(prefix+_make_random_string(6), _make_random_string(random()%944 + 80));
	// for (int i=0; i < count; ++i){
	// 	std::string key = prefix+_make_random_string(6);
	// 	std::string val = _make_random_string(random()%944 + 80);
	// 	kvs.emplace_back(key,val);
	// 	kvs.emplace_back(key, _make_random_string(random()%944 + 80));
	// }
	// Build an unordered_map
	std::unordered_map<std::string, std::string> um;
	for (const auto& act : kvs)
		um[act.first] = act.second;	

	for (const auto& act : kvs) {
		auto k = _make_slice(act.first);
		auto v = _make_slice(act.second);
		db->Set(k, v);
	}
    int cnt = 0;
	// Compare results
	for (const auto& kv : um) {
		auto k = _make_slice(kv.first);
		std::string v;
		db->Get(k, &v);
        fprintf(key_file, "%s\n", k.data());
        fflush(key_file);
		if (kv.second != v) {
            ++ cnt;
			printf("Value mismatch. Key: %s\nValue1: %s\nValue2: %s\n", kv.first.c_str(), kv.second.c_str(), v.c_str());
            cout<<"Value1 length "<<kv.second.length()<<"  Value2 lenght   "<<v.size()<<endl;
            fprintf(err_file, "%s\n", k.data());
            fflush(err_file);
		}
        else {
            fprintf(right_file, "%s\n", k.data());
            fflush(right_file);
        }
	}
    cout << cnt << " fails" << endl;
}

void test_press(const int count) {
	srand(time(NULL));

	puts("Pressure testing starts.");

	for (int i = 0; i < count; ++i) {
		auto k = _make_slice(_make_random_string(16));
		auto v = _make_slice(_make_random_string(80));
		
		auto res = db->Set(k, v);
		if (res != Ok) {
			printf("Set: Failed. Key: %s, Value: %s.\n", k.to_string().c_str(), v.to_string().c_str());
		}

		delete k.data();
		delete v.data();
	}
}
void test_correct2(int a){
    srand(time(NULL));
    char k[16];
    for(int i=0;i<16;i++)
        k[i] = '1';
    std::vector<std::pair<std::string, std::string>> kvs;
    if(a == '0'){
        puts("test persist start!   set random val now");
        for (int i=0;i<10;i++){
            char tmp[16];
            memcpy(tmp,k,16);
            tmp[15] = '0' + i;
            string key(tmp,16);
            string val = _make_random_string(random()%(944) + 80);
            kvs.emplace_back(key,val);
            for (const auto& act : kvs) {
		        auto k = _make_slice(act.first);
		        auto v = _make_slice(act.second);
		        db->Set(k, v);
	        }
        }
        exit(1);
    }
    else if(a == '1'){
        puts("test persist!   update val now");
        for (int i=0;i<10;i++){
            char tmp[16];
            memcpy(tmp,k,16);
            char vv[i*100+80];
            fill(vv,vv+i*100+80,'1');
            tmp[15] = '0' + i;
            string key(tmp,16);
            string val(vv,i*100+80);
            kvs.emplace_back(key,val);
            for (const auto& act : kvs) {
		        auto k = _make_slice(act.first);
		        auto v = _make_slice(act.second);
		        db->Set(k, v);
	        }
        }
        exit(1);
    }
    else{
        puts("test persist!   get val now");
        for (int i=0;i<10;i++){
            char tmp[16];
            memcpy(tmp,k,16);
            tmp[15] = '0' + i;
            string key(tmp,16);
            char vv[i*100+80];
            fill(vv,vv+i*100+80,'1');
            string v;
            db->Get(_make_slice(key),&v);
            if (memcmp(v.c_str(),vv,i*100+80)!=0){
                cout<<"persist failt\n";
                cout<<"v:"<<v<<"\nv length"<<v.length()<<endl;
            }
        }
    }
    exit(1);
}
void test_correct1(){
    srand(time(NULL));

	puts("test key set then get start!");
	// Generate some actions
	std::vector<std::pair<std::string, std::string>> kvs;
    for (int i=0;i<10;i++){
        kvs.emplace_back(_make_random_string(16), _make_random_string(random()%944 + 80));
    }
    std::unordered_map<std::string, std::string> um;
	for (const auto& act : kvs)
		um[act.first] = act.second;	
    for (const auto& act : kvs) {
		auto k = _make_slice(act.first);
		auto v = _make_slice(act.second);
		db->Set(k, v);
	}
    for (const auto& kv : um) {
		auto k = _make_slice(kv.first);
		std::string v;
		db->Get(k, &v);
		if (kv.second != v) {
			printf("Value mismatch. Key: %s\nValue1: %s\nValue2: %s\n", kv.first.c_str(), kv.second.c_str(), v.c_str());
            cout<<"Value1 length "<<kv.second.length()<<"  Value2 lenght   "<<v.size()<<endl;
		}
	}
    puts("test update key lenght update bigger");
    kvs.clear();
    um.clear();
    for (int i=0;i<10;i++){
        std::string key = _make_random_string(16);
        int block_num = random()%3 + 1;
        std::string val = _make_random_string(random()%(124*block_num - 80) + 80);
        kvs.emplace_back(key,val);
        block_num = random() % (8 - block_num) + block_num + 1;
        val = _make_random_string(random()%(124*block_num - 80) + 80);
        kvs.emplace_back(key,val);
    }
    for (const auto& act : kvs)
		um[act.first] = act.second;	
    for (const auto& act : kvs) {
		auto k = _make_slice(act.first);
		auto v = _make_slice(act.second);
		db->Set(k, v);
	}
    for (const auto& kv : um) {
		auto k = _make_slice(kv.first);
		std::string v;
		db->Get(k, &v);
		if (kv.second != v) {
			printf("Value mismatch. Key: %s\nValue1: %s\nValue2: %s\n", kv.first.c_str(), kv.second.c_str(), v.c_str());
            cout<<"Value1 length "<<kv.second.length()<<"  Value2 lenght   "<<v.size()<<endl;
		}
	}
    puts("test update key lenght update smaller");
    kvs.clear();
    um.clear();
    for (int i=0;i<10;i++){
        std::string key = _make_random_string(16);
        int block_num = random()%6+3;
        std::string val = _make_random_string(random()%(124*block_num - 80) + 80);
        kvs.emplace_back(key,val);
        block_num = random() % (block_num-2) + 1;
        val = _make_random_string(random()%(124*block_num - 80) + 80);
        kvs.emplace_back(key,val);
    }
    for (const auto& act : kvs)
		um[act.first] = act.second;	
    for (const auto& act : kvs) {
		auto k = _make_slice(act.first);
		auto v = _make_slice(act.second);
		db->Set(k, v);
	}
    for (const auto& kv : um) {
		auto k = _make_slice(kv.first);
		std::string v;
		db->Get(k, &v);
		if (kv.second != v) {
			printf("Value mismatch. Key: %s\nValue1: %s\nValue2: %s\n", kv.first.c_str(), kv.second.c_str(), v.c_str());
            cout<<"Value1 length "<<kv.second.length()<<"  Value2 lenght   "<<v.size()<<endl;
		}
	}
    puts("test update key but length don't change");
    kvs.clear();
    um.clear();
    for (int i=0;i<10;i++){
        std::string key = _make_random_string(16);
        int length = random()%(1024-80) + 80;
        std::string val = _make_random_string(length);
        kvs.emplace_back(key,val);
        val = _make_random_string(length);
        kvs.emplace_back(key,val);
    }
    for (const auto& act : kvs)
		um[act.first] = act.second;	
    for (const auto& act : kvs) {
		auto k = _make_slice(act.first);
		auto v = _make_slice(act.second);
		db->Set(k, v);
	}
    for (const auto& kv : um) {
		auto k = _make_slice(kv.first);
		std::string v;
		db->Get(k, &v);
		if (kv.second != v) {
			printf("Value mismatch. Key: %s\nValue1: %s\nValue2: %s\n", kv.first.c_str(), kv.second.c_str(), v.c_str());
            cout<<"Value1 length "<<kv.second.length()<<"  Value2 lenght   "<<v.size()<<endl;
		}
	}
}
int main(int argc, char* argv[]) {
	if (argc < 2) {
        FILE * log_file =  fopen("./performance.log", "w");
	    DB::CreateOrOpen("DB", &db, log_file);
		test_correct1();
        return 0;
	}
    if (argc < 3) {
        FILE * log_file =  fopen("./performance.log", "w");
	    DB::CreateOrOpen("DB", &db, log_file);
		test_correct2(*argv[1]);
        return 0;
    }
    
	std::string func = std::string(argv[1]);
	int count = std::atoi(argv[2]);

	// Build Tair DB!
	FILE * log_file =  fopen("./performance.log", "w");

	DB::CreateOrOpen("DB", &db, log_file);

	if (func == "correct")
		test_correctness(count);
	else if (func == "press")
		test_press(count);
	else
		test_correct1();
	return 0;
}
