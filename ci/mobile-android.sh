#!/usr/bin/env bash
# Runs on the Android emulator of the `mobile-android` job (Block 58, D2),
# as android-emulator-runner's `script`: prints the device program's
# NEEDED entries, pushes the program and the library's unit-test binary,
# runs the unit tests there (D4), then the host harness, which runs the
# program through `adb shell`.
#
# MOBILE_DEVICE_PROGRAM and MOBILE_LIB_TESTS are the binaries cargo-ndk
# built; FERRIFUGE_MOBILE_ARTIFACTS is where the output goes.
set -euo pipefail

dir=/data/local/tmp
readelf="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-readelf"
mkdir -p "$FERRIFUGE_MOBILE_ARTIFACTS"

echo "== NEEDED of the device program"
"$readelf" -d "$MOBILE_DEVICE_PROGRAM" | grep NEEDED

adb push "$MOBILE_DEVICE_PROGRAM" "$dir/mobile_device"
adb push "$MOBILE_LIB_TESTS" "$dir/lib_tests"
adb shell chmod 755 "$dir/mobile_device" "$dir/lib_tests"
# bolero's fuzz tests find their corpus from their source file's relative
# path (file!()), from the directory they run in, and the source checks
# read it there.
adb push "$GITHUB_WORKSPACE/src" "$dir/"

# The source checks find that src/ through source_root(), the directory
# the binary runs in when the crate's own is absent (D4): no test is
# skipped.
echo "== unit tests on the emulator"
adb shell "cd $dir && TMPDIR=$dir ./lib_tests" \
  2>&1 | tee "$FERRIFUGE_MOBILE_ARTIFACTS/unit-tests.txt"

echo "== the device program, through the harness"
FERRIFUGE_MOBILE=android FERRIFUGE_MOBILE_BINARY="$dir/mobile_device" \
  timeout -k 10 600 cargo test --test mobile \
  2>&1 | tee "$FERRIFUGE_MOBILE_ARTIFACTS/harness.txt"
