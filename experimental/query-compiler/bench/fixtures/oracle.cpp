#include <cstdint>
#include <cstdio>
#include <vector>
#include <algorithm>
#include <climits>
#include <cstring>
#include <cstdlib>

struct State {
    int64_t count = 0, sum = 0, sumv = 0, min = LLONG_MAX, max = LLONG_MIN;
    void add(int64_t value, int64_t joined) {
        count++;
        sum += joined;
        sumv += value;
        min = std::min(min, joined);
        max = std::max(max, joined);
    }
};

int main(int argc, char **argv) {
    const bool hot = argc >= 2 && std::strcmp(argv[1], "--hot") == 0;
    if (argc > 3 || (argc != 1 && !hot)) return 1;
    int64_t every = 10;
    if (argc == 3) {
        char *end;
        every = std::strtoll(argv[2], &end, 10);
        if (*end || every < 1) return 1;
    }
    std::vector<State> low(131), high(262144);
    for (int64_t x = 1; x <= 500000000; x++) {
        int64_t k = hot ? (x % every ? 0 : (x / every) * 15485863 % 262144) : x * 15485863 % 262144;
        int64_t n = (k * 73 + 19) % 4096, v = x % 97 - 48, m = v + k % 13 + n % 11;
        int id = k % 19 < 4 ? k % 19 : 4 + k % 127;
        low[id].add(v, m);
        high[k].add(v, m);
    }
    for (int mode = 0; mode < 2; mode++) {
        auto &states = mode ? high : low;
        for (unsigned i = 0; i < states.size(); i++) {
            auto &s = states[i];
            std::printf("%d %u %lld %lld %lld %lld %lld\n", mode, i, (long long)s.count,
                        (long long)s.sum, (long long)s.min, (long long)s.max, (long long)s.sumv);
        }
    }
}
