//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  //虽然抽象基类不会被创建对象，但是子类需要调用父类的构造函数来初始化子类的父类成员部分！
  //所以我们还是要在父类的构造函数中进行一些成员的初始化
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) {
    this->linear_ = new T[rows * cols];
    if (this->linear_ == nullptr) {
      LOG_ERROR("new linear error!");
    }
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  //纯虚函数，无需实现，说明这个类是个抽象类，子类需要实现纯虚函数
  // 末尾加上const，说明这个函数是 read only，不会修改 数据成员，且不能调用 非const成员函数。
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  // virtual ~Matrix() = default;
  virtual ~Matrix() { delete[] linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    if (data_) {
      for (int i = 0; i < rows; i++) {
        data_[i] = new T[cols];
        if (!data_[i]) {
          LOG_ERROR("new data[%d] error!", i);
        }
      }
    } else {
      LOG_ERROR("new data_ error!");
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    // LOG_DEBUG("getElement: i= %d  j = %d", i, j);
    if (i < 0 || j < 0 || i > this->rows_ - 1 || j > this->cols_ - 1) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is wrong");
    }
    return data_[i][j];
    // throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || j < 0 || i > this->rows_ - 1 || j > this->cols_ - 1) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is wrong");
    }
    data_[i][j] = val;
    this->linear_[this->cols_ * i + j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    std::vector<int>::size_type total = this->rows_ * this->cols_;
    if (source.size() != total) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "source is incorrect size");
    }
    for (std::vector<int>::size_type i = 0; i < total; ++i) {
      this->linear_[i] = source[i];
    }
    for (std::vector<int>::size_type i = 0; i < total; ++i) {
      data_[i / this->cols_][i % this->cols_] = source[i];
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    for (int i = 0; i < this->rows_; i++) {
      delete[] data_[i];
    }
    delete[] data_;
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetRowCount() != matrixB->GetRowCount() || matrixB->GetColumnCount() != matrixA->GetColumnCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int rows = matrixA->GetRowCount();
    int cols = matrixA->GetColumnCount();
    auto res = std::make_unique<RowMatrix<T>>(rows, cols);
    for (int i = 0; i < rows; ++i) {
      for (int j = 0; j < cols; ++j) {
        T sum = matrixA->GetElement(i, j) + matrixB->GetElement(i, j);
        res->SetElement(i, j, sum);
      }
    }
    return res;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetColumnCount() != matrixB->GetRowCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int rows = matrixA->GetRowCount();
    int cols = matrixB->GetColumnCount();
    auto res = std::make_unique<RowMatrix<T>>(rows, cols);
    for (int i = 0; i < rows; ++i) {
      for (int j = 0; j < cols; ++j) {
        T sum{};
        for (int z = 0; z < matrixA->GetColumnCount(); ++z) {
          if (z == 0) {
            sum = matrixA->GetElement(i, z) * matrixB->GetElement(z, j);
          } else {
            sum += matrixA->GetElement(i, z) * matrixB->GetElement(z, j);
          }
        }
        res->SetElement(i, j, sum);
      }
    }
    return res;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    auto res1 = RowMatrix<T>::Multiply(matrixA, matrixB);
    if (res1 == nullptr) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    auto res2 = RowMatrix<T>::Add(res1.get(), matrixC);
    return res2;
  }
};
}  // namespace bustub
