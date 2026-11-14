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
# path (file!()), from the directory they run in.
adb push "$GITHUB_WORKSPACE/src" "$dir/"

# The source checks read the checkout by its absolute path, which the
# emulator lacks; they run on the host (D4).
echo "== unit tests on the emulator"
adb shell "cd $dir && TMPDIR=$dir ./lib_tests \
  --skip the_adapter_spawns_only_through_its_runtime_handle \
  --skip only_the_tokio_module_and_the_seam_name_tokio_s_runtime_timers_and_sockets \
  --skip the_machine_reaches_the_encodings_only_through_the_codec \
  --skip test_util_gates_this_module_only_and_no_hook" \
  2>&1 | tee "$FERRIFUGE_MOBILE_ARTIFACTS/unit-tests.txt"

echo "== the device program, through the harness"
FERRIFUGE_MOBILE=android FERRIFUGE_MOBILE_BINARY="$dir/mobile_device" \
  timeout -k 10 600 cargo test --test mobile \
  2>&1 | tee "$FERRIFUGE_MOBILE_ARTIFACTS/harness.txt"
