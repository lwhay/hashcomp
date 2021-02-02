char *YCSB_command[5] = {"READ", "INSERT", "DELETE", "UPDATE", "SCAN"};

enum YCSB_operator {
    lookup = 0,
    insert,
    erease,
    update,
    scan
};

class YCSB_request {
private:
    YCSB_operator op;
    char *key;
    size_t ks;
    char *val;
    size_t vs;
public:
    YCSB_request(YCSB_operator _op, char *_key, size_t _ks, char *_val = nullptr, size_t _vs = 0) : op(_op), key(_key),
                                                                                                    ks(_ks), val(_val),
                                                                                                    vs(_vs) {}

    ~YCSB_request() {
        delete key;
        delete val;
    }

    YCSB_operator getOp() { return op; }

    char *getKey() { return key; }

    size_t keyLength() { return ks; }

    char *getVal() { return val; }

    size_t valLength() { return vs; }
};

class YCSBLoader {
private:
    const char *inputpath;
    size_t numberOfRequests;
    size_t limitOfRequests;

    void supersplit(const std::string &s, std::vector<std::string> &v, const std::string &c,
                    size_t n = std::numeric_limits<size_t>::max()) {
        std::string::size_type pos1, pos2;
        size_t len = s.length();
        pos2 = s.find(c);
        pos1 = 0;
        size_t found = 0;
        while (std::string::npos != pos2) {
            if (found++ == n) {
                v.emplace_back(s.substr(pos1));
                break;
            }
            v.emplace_back(s.substr(pos1, pos2 - pos1));
            pos1 = pos2 + c.size();
            pos2 = s.find(c, pos1);
        }
        if (pos1 != len)
            v.emplace_back(s.substr(pos1));
    }

public:
    YCSBLoader(const char *path, size_t number = std::numeric_limits<size_t>::max()) : inputpath(path),
                                                                                       numberOfRequests(0),
                                                                                       limitOfRequests(number) {}

    std::vector<YCSB_request *> load() {
        std::vector<YCSB_request *> requests;
        std::fstream lf(inputpath, ios::in);
        if(lf.fail()){
            printf("open file fail\n");
            //return 0;
        }
        string line;
        while (std::getline(lf, line)) {
            std::vector<std::string> fields;
            supersplit(line, fields, " ", 3);
            if (fields.size() < 2) continue;
            for (int i = 0; i < 5; i++) {
                if (fields[0].compare(YCSB_command[i]) == 0) {
                    if (i % 2 == 0) {
                        requests.push_back(
                                new YCSB_request(static_cast<YCSB_operator>(i), strdup(fields[2].substr(4).c_str()),
                                                 fields[2].length()));
                    } else {
                        requests.push_back(
                                new YCSB_request(static_cast<YCSB_operator>(i), strdup(fields[2].substr(4).c_str()),
                                                 fields[2].length(), strdup(fields[3].c_str()), fields[3].length()));
                    }
                    if (++numberOfRequests == limitOfRequests) goto complete;
                }
            }
        }
        complete:
        lf.close();
        return requests;
    }

    size_t size() { return numberOfRequests; }
};
