/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this file,
  You can obtain one at https://mozilla.org/MPL/2.0/.

  https://www.mozilla.org/en-US/MPL/2.0/FAQ/

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/


#ifndef FST_DB_H
#define FST_DB_H

// std includes
#include <vector>

// fst includes
#include "ifsttable.h"


class FstDb;

class FstTableProxy
{
public:

  FstTableProxy(FstDb fst_db, std::string table_name);

  ~FstTableProxy();

  void rename(std::string table_name);

  void arrange();
  void distinct();
  void do();
  void filter();
  void intersect();
  void mutate();
  void select();
  void pull();
  void sample_frac();
  void sample_n();
  void setdiff();
  void setequal();
  void slice();
  void summarise();
  void union();
  void union_all();

  void group_by();
  void group_indices();
  void group_size();
  void groups();
  void n_groups();
  void ungroup();

  void anti_join();
  void full_join();
  void inner_join();
  void left_join();
  void right_join();
  void semi_join();

  void collapse();
  void compute();
};


// A database of fst files with hierachiel structure
class FstDb
{

  public:

    FstDb(std::string path);

    ~FstDb() { }

    void add_table(IFstTable table, std::string table_name) const;

    void remove_table(std::string table_name) const;

    std::vector<table_info> table_index(std::string path) const;
};


#endif  // FST_DB_H
