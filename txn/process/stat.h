// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "flags.h"

static bool comparator(const std::pair<std::string, int>& p1, const std::pair<std::string, int>& p2) {
  return p1.second > p2.second;  // Descending order
}

struct T_K {
  table_id_t table_id;
  itemkey_t key;

  bool operator<(const T_K& other) const {
    if (this->table_id == other.table_id) {
      return this->key < other.key;
    } else {
      return this->table_id < other.table_id;
    }
  }
};

enum KeyType : int {
  kKeyRead = 0,
  kKeyWrite,
  kKeyCommit
};

static bool comparator2(const std::pair<T_K, uint64_t>& p1, const std::pair<T_K, uint64_t>& p2) {
  return p1.second > p2.second;  // Descending order
}

class EventCount {
 public:
  void RegEvent(int t, const std::string& txn_name, const std::string& event_name) {
#if OUTPUT_EVENT_STAT
    if (txn_name == "no") {
      auto& map = event_cnt[t];
      auto res = map.find(event_name);
      if (res == map.end()) {
        // no such event
        map[event_name] = 1;
      } else {
        // have such event
        res->second++;
      }
    }
#endif
  }

  void Output(std::ofstream& of) {
    of << "============== Event & Count ===============" << std::endl;

    for (int i = 0; i < MAX_TNUM_PER_CN; i++) {
      if (!event_cnt[i].empty()) {
        for (auto event : event_cnt[i]) {
          auto res = sum_event_cnt.find(event.first);
          if (res == sum_event_cnt.end()) {
            sum_event_cnt[event.first] = event.second;
          } else {
            sum_event_cnt[event.first] += event.second;
          }
        }
      }
    }

    std::vector<std::pair<std::string, int>> sum_arr;
    for (auto event : sum_event_cnt) {
      sum_arr.push_back(event);
    }

    sort(sum_arr.begin(), sum_arr.end(), comparator);

    of << "====== Sum ======" << std::endl;
    for (auto i : sum_arr) {
      of << i.first << " : " << i.second << std::endl;
    }

    of << std::endl;

    of << "====== Details ======" << std::endl;

    for (int i = 0; i < MAX_TNUM_PER_CN; i++) {
      if (!event_cnt[i].empty()) {
        std::vector<std::pair<std::string, int>> arr;

        for (auto event : event_cnt[i]) {
          arr.push_back(event);
        }

        sort(arr.begin(), arr.end(), comparator);

        of << "Thread " << i << std::endl;
        for (auto i : arr) {
          of << i.first << " : " << i.second << std::endl;
        }
        of << std::endl;
      }
    }
  }

 private:
  std::unordered_map<std::string, int> event_cnt[MAX_TNUM_PER_CN];
  std::unordered_map<std::string, int> sum_event_cnt;
};

class KeyCount {
 public:
  void RegKey(int t, KeyType type, const std::string& txn_name, table_id_t tab, itemkey_t k) {
    auto& map = (type == kKeyRead) ? read_key_cnt[t] : ((type == kKeyWrite) ? write_key_cnt[t] : (type == kKeyCommit ? commit_key_cnt[t] : read_key_cnt[t]));
    T_K t_k;
    t_k.table_id = tab;
    t_k.key = k;
    auto res = map.find(t_k);
    if (res == map.end()) {
      // no such key
      map[t_k] = 1;
    } else {
      // have such key
      res->second++;
    }
  }

  void Output() {
    std::ofstream of("../../../key_count.yml", std::ofstream::out);
    of << "============== Key & Count ===============" << std::endl;

    for (int i = 0; i < MAX_TNUM_PER_CN; i++) {
      if (!write_key_cnt[i].empty()) {
        for (auto wkey : write_key_cnt[i]) {
          auto res = sum_write_key_cnt.find(wkey.first);
          if (res == sum_write_key_cnt.end()) {
            sum_write_key_cnt[wkey.first] = wkey.second;
          } else {
            sum_write_key_cnt[wkey.first] += wkey.second;
          }
        }
      }

      if (!read_key_cnt[i].empty()) {
        for (auto rkey : read_key_cnt[i]) {
          auto res = sum_read_key_cnt.find(rkey.first);
          if (res == sum_read_key_cnt.end()) {
            sum_read_key_cnt[rkey.first] = rkey.second;
          } else {
            sum_read_key_cnt[rkey.first] += rkey.second;
          }
        }
      }

      if (!commit_key_cnt[i].empty()) {
        for (auto ckey : commit_key_cnt[i]) {
          auto res = sum_commit_key_cnt.find(ckey.first);
          if (res == sum_commit_key_cnt.end()) {
            sum_commit_key_cnt[ckey.first] = ckey.second;
          } else {
            sum_commit_key_cnt[ckey.first] += ckey.second;
          }
        }
      }
    }

    std::vector<std::pair<T_K, uint64_t>> sum_write_key_arr;
    std::vector<std::pair<T_K, uint64_t>> sum_read_key_arr;
    std::vector<std::pair<T_K, uint64_t>> sum_commit_key_arr;

    for (auto w_key : sum_write_key_cnt) {
      sum_write_key_arr.push_back(w_key);
    }

    for (auto r_key : sum_read_key_cnt) {
      sum_read_key_arr.push_back(r_key);
    }

    for (auto c_key : sum_commit_key_cnt) {
      sum_commit_key_arr.push_back(c_key);
    }

    sort(sum_write_key_arr.begin(), sum_write_key_arr.end(), comparator2);

    of << "====== Sum Try Write Key ======" << std::endl;
    for (auto i : sum_write_key_arr) {
      of << "table: " << i.first.table_id << ", key: " << i.first.key << ", cnt: " << i.second << std::endl;
    }

    of << std::endl;

    of << "====== Sum Commit Key ======" << std::endl;
    for (auto i : sum_commit_key_arr) {
      of << "table: " << i.first.table_id << ", key: " << i.first.key << ", cnt: " << i.second << std::endl;
    }

    of << std::endl;

    of << "====== Sum Read Key ======" << std::endl;
    for (auto i : sum_read_key_arr) {
      of << "table: " << i.first.table_id << ", key: " << i.first.key << ", cnt: " << i.second << std::endl;
    }

    of << std::endl;

    // of << "====== Details ======" << std::endl;

    // for (int i = 0; i < MAX_TNUM_PER_CN; i++) {
    //   if (!event_cnt[i].empty()) {
    //     std::vector<std::pair<std::string, int>> arr;

    //     for (auto event : event_cnt[i]) {
    //       arr.push_back(event);
    //     }

    //     sort(arr.begin(), arr.end(), comparator);

    //     of << "Thread " << i << std::endl;
    //     for (auto i : arr) {
    //       of << i.first << " : " << i.second << std::endl;
    //     }
    //     of << std::endl;
    //   }
    // }
    of.close();
  }

 private:
  std::map<T_K, uint64_t> write_key_cnt[MAX_TNUM_PER_CN];
  std::map<T_K, uint64_t> commit_key_cnt[MAX_TNUM_PER_CN];
  std::map<T_K, uint64_t> read_key_cnt[MAX_TNUM_PER_CN];

  std::map<T_K, uint64_t> sum_write_key_cnt;
  std::map<T_K, uint64_t> sum_commit_key_cnt;
  std::map<T_K, uint64_t> sum_read_key_cnt;
};