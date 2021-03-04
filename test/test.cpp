#include <include/db.hpp>
#include <vector>
#include <unordered_set>
#include <iostream>
using namespace std;

#define TIMES (200)
#define BIG_PRIME (805306457)
char *random_str(unsigned int size) {
    char *str = (char *)malloc(size + 1);
    for (unsigned int i = 0; i < size; i++) {
        switch (rand() % 3) {
        case 0:
            str[i] = rand() % 10 + '0';
            break;
        case 1:
            str[i] = rand() % 26 + 'A';
            break;
        case 2:
            str[i] = rand() % 26 + 'a';
            break;
        default:
            break;
        }
    }
    str[size] = 0;
    return str;
}
char* intTo_str(uint32_t k){
    char* str = (char*)malloc(16 + 1);
    char* tmp = (char*)malloc(4 + 1);
    tmp[0] = (char)k >> 24;
    tmp[1] = (char)(k >> 16) % (1 << 8);
    tmp[2] = (char)(k >> 8) % (1 << 8);
    tmp[3] = (char)k % (1 << 8);
    memcpy(str + 12, tmp, 4);
    memset(str, 0, 12);
    delete []tmp;
    str[16] = 0;
    return str;
}
int main() {
    DB *db = nullptr;
    DB::CreateOrOpen("./f", &db);
    std::unordered_set<char*> st;

    std::vector<std::pair<Slice, Slice>> kv;
    printf("write state:\n");
   /* for (int i = 0; i < TIMES; ++ i) {
        Slice k(random_str(16), 16);
        Slice v(random_str(80), 80);
        kv.push_back(std::make_pair(k, v));
        db->Set(k, v);
    }*/
    for (uint32_t i = 0; i < TIMES; ++ i){
        uint32_t t = i + 100;
        //std::cout << t << std::endl;
        if (st.find(intTo_str(t)) == st.end()) {
            std::cout << "111" << std::endl;
            st.insert(intTo_str(t));
            Slice k(intTo_str(t), 16);
            Slice v(random_str(80), 80);
            kv.push_back(std::make_pair(k, v));
            db->Set(k, v);
            printf("%d\n%s ", i, v.data());
        }
        

        if (st.find(intTo_str(t + BIG_PRIME)) == st.end()) {
            std::cout << "222" << std::endl;
            st.insert(intTo_str(t + BIG_PRIME));
            Slice k1(intTo_str(t + BIG_PRIME), 16);
            Slice v1(random_str(80), 80);
            kv.push_back(std::make_pair(k1, v1));
            db->Set(k1, v1);
            printf("%s\n", v1.data());
        }
        
    }
    printf("\nread stage:\n");
    for (int i = 0; i < kv.size(); ++ i) {
        std::string val;
        auto k = kv[i].first;
        auto v = kv[i].second;
        db->Get(k, &val);
        printf("%d\n%s\n%s\n", i, val.c_str(), v.data());
        if (strcmp(val.c_str(), v.data()) != 0)
            printf("not equal\n");
    }/*
    printf("\nmodify stage:\n");
    for (int i = 0; i < kv.size(); ++ i) {
        Slice ne(random_str(80), 80);
        auto k = kv[i].first;
        auto v = kv[i].second;
        db->Set(k, ne);
        std::string val;
        db->Get(k, &val);
        printf("%d\n%s\n%s\n%s\n", i, val.c_str(), ne.data(), v.data());
        if (strcmp(val.c_str(), ne.data()) != 0)
            printf("not equal\n");
    }*/



    // Slice k;
    // k.size() = 16;
    // Slice v;
    // v.size() = 80;
    // int times = 40;
    // while (times--) {
    //     k.data() = random_str(16);
    //     v.data() = random_str(80);
    //     db->Set(k, v);
    //     std::string a;
    //     db->Get(k, &a);
    //     printf("%d\n%s\n%s\n", 100 - times, a.c_str(), v.data());
    //     if (strcmp(a.c_str(), v.data()) != 0)
    //         printf("not equal\n");
    //     free(k.data());
    //     free(v.data());
    // }
    return 0;
}
