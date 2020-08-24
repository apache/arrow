// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <string>  // for string

#include "cpp11/R.hpp"         // for SEXP, SEXPREC, R_xlen_t, Rboolean, INT...
#include "cpp11/r_string.hpp"  // for r_string
#include "cpp11/r_vector.hpp"  // for r_vector
#include "cpp11/sexp.hpp"      // for sexp

namespace cpp11 {
template <typename V, typename T>
class matrix {
 private:
  V vector_;
  int nrow_;

 public:
  class row {
   private:
    matrix& parent_;
    int row_;

   public:
    row(matrix& parent, R_xlen_t row) : parent_(parent), row_(row) {}
    T operator[](const int pos) { return parent_.vector_[row_ + (pos * parent_.nrow_)]; }

    class iterator {
     private:
      row& row_;
      int pos_;

     public:
      iterator(row& row, R_xlen_t pos) : row_(row), pos_(pos) {}
      iterator begin() const { return row_.parent_.vector_iterator(&this, 0); }
      iterator end() const { return iterator(&this, row_.size()); }
      inline iterator& operator++() {
        ++pos_;
        return *this;
      }
      bool operator!=(const iterator& rhs) {
        return !(pos_ == rhs.pos_ && row_.row_ == rhs.row_.row_);
      }
      T operator*() const { return row_[pos_]; };
    };

    iterator begin() { return iterator(*this, 0); }
    iterator end() { return iterator(*this, size()); }
    R_xlen_t size() const { return parent_.vector_.size() / parent_.nrow_; }
    bool operator!=(const row& rhs) { return row_ != rhs.row_; }
    row& operator++() {
      ++row_;
      return *this;
    }
    row& operator*() { return *this; }
  };
  friend row;

 public:
  matrix(SEXP data) : vector_(data), nrow_(INTEGER_ELT(vector_.attr("dim"), 0)) {}

  template <typename V2, typename T2>
  matrix(const cpp11::matrix<V2, T2>& rhs) : vector_(rhs), nrow_(rhs.nrow()) {}

  matrix(int nrow, int ncol) : vector_(R_xlen_t(nrow * ncol)), nrow_(nrow) {
    vector_.attr("dim") = {nrow, ncol};
  }

  int nrow() const { return nrow_; }

  int ncol() const { return size() / nrow_; }

  SEXP data() const { return vector_.data(); }

  R_xlen_t size() const { return vector_.size(); }

  operator SEXP() const { return SEXP(vector_); }

  // operator sexp() { return sexp(vector_); }

  sexp attr(const char* name) const { return SEXP(vector_.attr(name)); }

  sexp attr(const std::string& name) const { return SEXP(vector_.attr(name)); }

  sexp attr(SEXP name) const { return SEXP(vector_.attr(name)); }

  r_vector<r_string> names() const { return SEXP(vector_.names()); }

  row operator[](const int pos) { return {*this, pos}; }

  T operator()(int row, int col) { return vector_[row + (col * nrow_)]; }

  row begin() { return {*this, 0}; }
  row end() { return {*this, nrow_}; }
};

using doubles_matrix = matrix<r_vector<double>, double>;
using integers_matrix = matrix<r_vector<int>, int>;
using logicals_matrix = matrix<r_vector<Rboolean>, Rboolean>;
using strings_matrix = matrix<r_vector<r_string>, r_string>;

namespace writable {
using doubles_matrix = matrix<r_vector<double>, r_vector<double>::proxy>;
using integers_matrix = matrix<r_vector<int>, r_vector<int>::proxy>;
using logicals_matrix = matrix<r_vector<Rboolean>, r_vector<Rboolean>::proxy>;
using strings_matrix = matrix<r_vector<r_string>, r_vector<r_string>::proxy>;
}  // namespace writable

// TODO: Add tests for Matrix class
}  // namespace cpp11
