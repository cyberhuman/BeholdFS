#!/bin/bash

init()
{
  echo "Initializing..."
  OPTIONS="debug=8"
  STORE="$(mktemp -d)" && MOUNT="$(mktemp -d)" && {
    echo "-store: $STORE"
    echo "-mount: $MOUNT"
    return 0
  } || {
    echo "-error"
    return 1
  }
}

cleanup()
{
  echo "Cleaning up..."
  rm -rf "$MOUNT" && rm -rf "$STORE" && {
    echo "-ok"
    return 0
  } || {
    echo "-error"
    return 1
  }
}

mount()
{
  echo "Mounting..."
  src/beholdfs -o "$OPTIONS" "$STORE" "$MOUNT" && {
    echo "-ok"
    return 0
  } || {
    echo "-error"
    return 1
  }
}

unmount()
{
  echo "Unmounting..."
  fusermount -u "$MOUNT" && {
    echo "-ok"
    return 0
  } || {
    echo "-error"
    return 1
  }
}

runtest()
{
  local TEST="$1"

  echo "===== Executing $TEST ====="

  init && {
    mount && {
      "test/$TEST.sh" "$MOUNT" "$STORE"
      unmount
    }
    cleanup
  }
  echo "----------------------------"
  echo
}

runtest 0000
runtest 0001

