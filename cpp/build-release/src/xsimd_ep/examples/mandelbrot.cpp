/***************************************************************************
 * Copyright (c) Johan Mabille, Sylvain Corlay, Wolf Vollprecht and         *
 * Martin Renou                                                             *
 * Copyright (c) QuantStack                                                 *
 * Copyright (c) Serge Guelton                                              *
 *                                                                          *
 * Distributed under the terms of the BSD 3-Clause License.                 *
 *                                                                          *
 * The full license is in the file LICENSE, distributed with this software. *
 ****************************************************************************/

// This file is derived from tsimd (MIT License)
// https://github.com/ospray/tsimd/blob/master/benchmarks/mandelbrot.cpp
// Author Jefferson Amstutz / intel

#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "pico_bench.hpp"

#include <xsimd/xsimd.hpp>

// helper function to write the rendered image as PPM file
inline void writePPM(const std::string& fileName,
                     const int sizeX,
                     const int sizeY,
                     const int* pixel)
{
    FILE* file = fopen(fileName.c_str(), "wb");
    fprintf(file, "P6\n%i %i\n255\n", sizeX, sizeY);
    unsigned char* out = (unsigned char*)alloca(3 * sizeX);
    for (int y = 0; y < sizeY; y++)
    {
        const unsigned char* in = (const unsigned char*)&pixel[(sizeY - 1 - y) * sizeX];

        for (int x = 0; x < sizeX; x++)
        {
            out[3 * x + 0] = in[4 * x + 0];
            out[3 * x + 1] = in[4 * x + 1];
            out[3 * x + 2] = in[4 * x + 2];
        }

        fwrite(out, 3 * sizeX, sizeof(char), file);
    }
    fprintf(file, "\n");
    fclose(file);
}

namespace xsimd
{

    template <class arch>
    inline batch<int, arch> mandel(const batch_bool<float, arch>& _active,
                                   const batch<float, arch>& c_re,
                                   const batch<float, arch>& c_im,
                                   int maxIters)
    {
        using float_batch_type = batch<float, arch>;
        using int_batch_type = batch<int, arch>;

        constexpr std::size_t N = float_batch_type::size;

        float_batch_type z_re = c_re;
        float_batch_type z_im = c_im;
        int_batch_type vi(0);

        for (int i = 0; i < maxIters; ++i)
        {
            auto active = _active & ((z_re * z_re + z_im * z_im) <= float_batch_type(4.f));
            if (!xsimd::any(active))
            {
                break;
            }

            float_batch_type new_re = z_re * z_re - z_im * z_im;
            float_batch_type new_im = 2.f * z_re * z_im;

            z_re = c_re + new_re;
            z_im = c_im + new_im;

            vi = select(batch_bool_cast<int>(active), vi + 1, vi);
        }

        return vi;
    }

    template <class arch>
    void mandelbrot(float x0,
                    float y0,
                    float x1,
                    float y1,
                    int width,
                    int height,
                    int maxIters,
                    int output[])
    {
        using float_batch_type = batch<float, arch>;
        using int_batch_type = batch<int, arch>;

        constexpr std::size_t N = float_batch_type::size;
        float dx = (x1 - x0) / width;
        float dy = (y1 - y0) / height;

        float arange[N];
        std::iota(&arange[0], &arange[N], 0.f);
        // float_batch_type programIndex(&arange[0], xsimd::aligned_mode());

        auto programIndex = float_batch_type::load(&arange[0], xsimd::aligned_mode());
        // std::iota(programIndex.begin(), programIndex.end(), 0.f);

        for (int j = 0; j < height; j++)
        {
            for (int i = 0; i < width; i += N)
            {
                float_batch_type x(x0 + (i + programIndex) * dx);
                float_batch_type y(y0 + j * dy);

                auto active = x < float_batch_type(width);

                int base_index = (j * width + i);
                auto result = mandel<arch>(active, x, y, maxIters);

                // implement masked store!
                // xsimd::store_aligned(result, output + base_index, active);
                int_batch_type prev_data = int_batch_type::load_unaligned(output + base_index);
                select(batch_bool_cast<int>(active), result, prev_data)
                    .store_aligned(output + base_index);
            }
        }
    }

} // namespace xsimd

// omp version ////////////////////////////////////////////////////////////////

namespace omp
{

#pragma omp declare simd
    template <typename T>
    inline int mandel(T c_re, T c_im, int count)
    {
        T z_re = c_re, z_im = c_im;
        int i;
        for (i = 0; i < count; ++i)
        {
            if (z_re * z_re + z_im * z_im > 4.f)
            {
                break;
            }

            T new_re = z_re * z_re - z_im * z_im;
            T new_im = 2.f * z_re * z_im;
            z_re = c_re + new_re;
            z_im = c_im + new_im;
        }

        return i;
    }

    void mandelbrot(float x0, float y0, float x1, float y1, int width,
                    int height, int maxIterations, int output[])
    {
        float dx = (x1 - x0) / width;
        float dy = (y1 - y0) / height;

        for (int j = 0; j < height; j++)
        {

#pragma omp simd
            for (int i = 0; i < width; ++i)
            {
                float x = x0 + i * dx;
                float y = y0 + j * dy;

                int index = (j * width + i);
                output[index] = mandel<float>(x, y, maxIterations);
            }
        }
    }

} // namespace omp

// scalar version /////////////////////////////////////////////////////////////

namespace scalar
{

    inline int mandel(float c_re, float c_im, int count)
    {
        float z_re = c_re, z_im = c_im;
        int i;
        for (i = 0; i < count; ++i)
        {
            if (z_re * z_re + z_im * z_im > 4.f)
            {
                break;
            }

            float new_re = z_re * z_re - z_im * z_im;
            float new_im = 2.f * z_re * z_im;
            z_re = c_re + new_re;
            z_im = c_im + new_im;
        }

        return i;
    }

    void mandelbrot(float x0, float y0, float x1, float y1,
                    int width, int height, int maxIterations, int output[])
    {
        float dx = (x1 - x0) / width;
        float dy = (y1 - y0) / height;

        for (int j = 0; j < height; j++)
        {
            for (int i = 0; i < width; ++i)
            {
                float x = x0 + i * dx;
                float y = y0 + j * dy;

                int index = (j * width + i);
                output[index] = mandel(x, y, maxIterations);
            }
        }
    }

} // namespace scalar

// run simd version of mandelbrot benchmark for a specific arch
template <class arch, class bencher_t>
void run_arch(
    bencher_t& bencher,
    float x0,
    float y0,
    float x1,
    float y1,
    int width,
    int height,
    int maxIters,
    std::vector<int, xsimd::aligned_allocator<int>>& buffer)
{
    std::fill(buffer.begin(), buffer.end(), 0);
    auto stats = bencher([&]()
                         { xsimd::mandelbrot<arch>(x0, y0, x1, y1, width, height, maxIters, buffer.data()); });

    const float scalar_min = stats.min().count();

    std::cout << '\n'
              << arch::name() << " " << stats << '\n';
    auto filename = std::string("mandelbrot_") + std::string(arch::name()) + std::string(".ppm");
    writePPM(filename.c_str(), width, height, buffer.data());
}

template <class T>
struct run_archlist;

// run simd version of mandelbrot benchmark for a list
// of archs
template <class... Arch>
struct run_archlist<xsimd::arch_list<Arch...>>
{
    template <class bencher_t>
    static void run(
        bencher_t& bencher,
        float x0,
        float y0,
        float x1,
        float y1,
        int width,
        int height,
        int maxIters,
        std::vector<int, xsimd::aligned_allocator<int>>& buffer)
    {
        using expand_type = int[];
        expand_type { (run_arch<Arch>(bencher, x0, y0, x1, x1, width, height, maxIters, buffer), 0)... };
    }
};

int main()
{
    using namespace std::chrono;

    const unsigned int width = 1024;
    const unsigned int height = 768;
    const float x0 = -2;
    const float x1 = 1;
    const float y0 = -1;
    const float y1 = 1;
    const int maxIters = 256;

    std::vector<int, xsimd::aligned_allocator<int>> buf(width * height);

    auto bencher = pico_bench::Benchmarker<milliseconds> { 64, seconds { 10 } };

    std::cout << "starting benchmarks (results in 'ms')... " << '\n';

    // scalar run ///////////////////////////////////////////////////////////////

    std::fill(buf.begin(), buf.end(), 0);

    auto stats_scalar = bencher([&]()
                                { scalar::mandelbrot(x0, y0, x1, y1, width, height, maxIters, buf.data()); });

    const float scalar_min = stats_scalar.min().count();

    std::cout << '\n'
              << "scalar " << stats_scalar << '\n';

    writePPM("mandelbrot_scalar.ppm", width, height, buf.data());

    // omp run //////////////////////////////////////////////////////////////////

    std::fill(buf.begin(), buf.end(), 0);

    auto stats_omp = bencher([&]()
                             { omp::mandelbrot(x0, y0, x1, y1, width, height, maxIters, buf.data()); });

    const float omp_min = stats_omp.min().count();

    std::cout << '\n'
              << "omp " << stats_omp << '\n';

    writePPM("mandelbrot_omp.ppm", width, height, buf.data());

    run_archlist<xsimd::supported_architectures>::run(bencher, x0, y0, x1, y1, width, height, maxIters, buf);

    return 0;
}
