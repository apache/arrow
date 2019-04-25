// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cpu_features_macros.h"

#if defined(CPU_FEATURES_ARCH_X86)
#include "cpuinfo_x86.h"
#elif defined(CPU_FEATURES_ARCH_ARM)
#include "cpuinfo_arm.h"
#elif defined(CPU_FEATURES_ARCH_AARCH64)
#include "cpuinfo_aarch64.h"
#elif defined(CPU_FEATURES_ARCH_MIPS)
#include "cpuinfo_mips.h"
#elif defined(CPU_FEATURES_ARCH_PPC)
#include "cpuinfo_ppc.h"
#endif

static void PrintEscapedAscii(const char* str) {
  putchar('"');
  for (; str && *str; ++str) {
    switch (*str) {
      case '\"':
      case '\\':
      case '/':
      case '\b':
      case '\f':
      case '\n':
      case '\r':
      case '\t':
        putchar('\\');
    }
    putchar(*str);
  }
  putchar('"');
}

static void PrintVoid(void) {}
static void PrintComma(void) { putchar(','); }
static void PrintLineFeed(void) { putchar('\n'); }
static void PrintOpenBrace(void) { putchar('{'); }
static void PrintCloseBrace(void) { putchar('}'); }
static void PrintOpenBracket(void) { putchar('['); }
static void PrintCloseBracket(void) { putchar(']'); }
static void PrintString(const char* field) { printf("%s", field); }
static void PrintAlignedHeader(const char* field) { printf("%-15s : ", field); }
static void PrintIntValue(int value) { printf("%d", value); }
static void PrintDecHexValue(int value) {
  printf("%3d (0x%02X)", value, value);
}
static void PrintJsonHeader(const char* field) {
  PrintEscapedAscii(field);
  putchar(':');
}

typedef struct {
  void (*Start)(void);
  void (*ArrayStart)(void);
  void (*ArraySeparator)(void);
  void (*ArrayEnd)(void);
  void (*PrintString)(const char* value);
  void (*PrintValue)(int value);
  void (*EndField)(void);
  void (*StartField)(const char* field);
  void (*End)(void);
} Printer;

static Printer getJsonPrinter(void) {
  return (Printer){
      .Start = &PrintOpenBrace,
      .ArrayStart = &PrintOpenBracket,
      .ArraySeparator = &PrintComma,
      .ArrayEnd = &PrintCloseBracket,
      .PrintString = &PrintEscapedAscii,
      .PrintValue = &PrintIntValue,
      .EndField = &PrintComma,
      .StartField = &PrintJsonHeader,
      .End = &PrintCloseBrace,
  };
}

static Printer getTextPrinter(void) {
  return (Printer){
      .Start = &PrintVoid,
      .ArrayStart = &PrintVoid,
      .ArraySeparator = &PrintComma,
      .ArrayEnd = &PrintVoid,
      .PrintString = &PrintString,
      .PrintValue = &PrintDecHexValue,
      .EndField = &PrintLineFeed,
      .StartField = &PrintAlignedHeader,
      .End = &PrintVoid,
  };
}

// Prints a named numeric value in both decimal and hexadecimal.
static void PrintN(const Printer p, const char* field, int value) {
  p.StartField(field);
  p.PrintValue(value);
  p.EndField();
}

// Prints a named string.
static void PrintS(const Printer p, const char* field, const char* value) {
  p.StartField(field);
  p.PrintString(value);
  p.EndField();
}

static int cmp(const void* p1, const void* p2) {
  return strcmp(*(const char* const*)p1, *(const char* const*)p2);
}

#define DEFINE_PRINT_FLAGS(HasFeature, FeatureName, FeatureType, LastEnum) \
  static void PrintFlags(const Printer p, const FeatureType* features) {   \
    size_t i;                                                              \
    const char* ptrs[LastEnum] = {0};                                      \
    size_t count = 0;                                                      \
    for (i = 0; i < LastEnum; ++i) {                                       \
      if (HasFeature(features, i)) {                                       \
        ptrs[count] = FeatureName(i);                                      \
        ++count;                                                           \
      }                                                                    \
    }                                                                      \
    qsort(ptrs, count, sizeof(char*), cmp);                                \
    p.StartField("flags");                                                 \
    p.ArrayStart();                                                        \
    for (i = 0; i < count; ++i) {                                          \
      if (i > 0) p.ArraySeparator();                                       \
      p.PrintString(ptrs[i]);                                              \
    }                                                                      \
    p.ArrayEnd();                                                          \
  }

#if defined(CPU_FEATURES_ARCH_X86)
DEFINE_PRINT_FLAGS(GetX86FeaturesEnumValue, GetX86FeaturesEnumName, X86Features,
                   X86_LAST_)
#elif defined(CPU_FEATURES_ARCH_ARM)
DEFINE_PRINT_FLAGS(GetArmFeaturesEnumValue, GetArmFeaturesEnumName, ArmFeatures,
                   ARM_LAST_)
#elif defined(CPU_FEATURES_ARCH_AARCH64)
DEFINE_PRINT_FLAGS(GetAarch64FeaturesEnumValue, GetAarch64FeaturesEnumName,
                   Aarch64Features, AARCH64_LAST_)
#elif defined(CPU_FEATURES_ARCH_MIPS)
DEFINE_PRINT_FLAGS(GetMipsFeaturesEnumValue, GetMipsFeaturesEnumName,
                   MipsFeatures, MIPS_LAST_)
#elif defined(CPU_FEATURES_ARCH_PPC)
DEFINE_PRINT_FLAGS(GetPPCFeaturesEnumValue, GetPPCFeaturesEnumName, PPCFeatures,
                   PPC_LAST_)
#endif

static void PrintFeatures(const Printer printer) {
#if defined(CPU_FEATURES_ARCH_X86)
  char brand_string[49];
  const X86Info info = GetX86Info();
  FillX86BrandString(brand_string);
  PrintS(printer, "arch", "x86");
  PrintS(printer, "brand", brand_string);
  PrintN(printer, "family", info.family);
  PrintN(printer, "model", info.model);
  PrintN(printer, "stepping", info.stepping);
  PrintS(printer, "uarch",
         GetX86MicroarchitectureName(GetX86Microarchitecture(&info)));
  PrintFlags(printer, &info.features);
#elif defined(CPU_FEATURES_ARCH_ARM)
  const ArmInfo info = GetArmInfo();
  PrintS(printer, "arch", "ARM");
  PrintN(printer, "implementer", info.implementer);
  PrintN(printer, "architecture", info.architecture);
  PrintN(printer, "variant", info.variant);
  PrintN(printer, "part", info.part);
  PrintN(printer, "revision", info.revision);
  PrintFlags(printer, &info.features);
#elif defined(CPU_FEATURES_ARCH_AARCH64)
  const Aarch64Info info = GetAarch64Info();
  PrintS(printer, "arch", "aarch64");
  PrintN(printer, "implementer", info.implementer);
  PrintN(printer, "variant", info.variant);
  PrintN(printer, "part", info.part);
  PrintN(printer, "revision", info.revision);
  PrintFlags(printer, &info.features);
#elif defined(CPU_FEATURES_ARCH_MIPS)
  (void)&PrintN;  // Remove unused function warning.
  const MipsInfo info = GetMipsInfo();
  PrintS(printer, "arch", "mips");
  PrintFlags(printer, &info.features);
#elif defined(CPU_FEATURES_ARCH_PPC)
  (void)&PrintN;  // Remove unused function warning.
  const PPCInfo info = GetPPCInfo();
  const PPCPlatformStrings strings = GetPPCPlatformStrings();
  PrintS(printer, "arch", "ppc");
  PrintS(printer, "platform", strings.platform);
  PrintS(printer, "model", strings.model);
  PrintS(printer, "machine", strings.machine);
  PrintS(printer, "cpu", strings.cpu);
  PrintS(printer, "instruction set", strings.type.platform);
  PrintS(printer, "microarchitecture", strings.type.base_platform);
  PrintFlags(printer, &info.features);
#endif
}

static void showUsage(const char* name) {
  printf(
      "\n"
      "Usage: %s [options]\n"
      "      Options:\n"
      "      -h | --help     Show help message.\n"
      "      -j | --json     Format output as json instead of plain text.\n"
      "\n",
      name);
}

int main(int argc, char** argv) {
  Printer printer = getTextPrinter();
  int i = 1;
  for (; i < argc; ++i) {
    const char* arg = argv[i];
    if (strcmp(arg, "-j") == 0 || strcmp(arg, "--json") == 0) {
      printer = getJsonPrinter();
    } else {
      showUsage(argv[0]);
      if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0)
        return EXIT_SUCCESS;
      return EXIT_FAILURE;
    }
  }
  printer.Start();
  PrintFeatures(printer);
  printer.End();
  PrintLineFeed();
  return EXIT_SUCCESS;
}
