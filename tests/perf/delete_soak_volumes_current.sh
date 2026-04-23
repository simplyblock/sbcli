#!/usr/bin/env bash
set -u

for id in \
  367b3fb4-13e5-41d6-88ad-d3c3d20a90e0 \
  6772e5e5-1e6b-4597-90ab-6a4e9921c171 \
  33bd0a47-0492-4e4e-8390-8abd1e8d358b \
  dbd23523-21a3-4cb9-8e6a-40f561b83865 \
  a5fd8d4e-764c-462e-9729-925b6b12fcf5 \
  b388c4cc-fea6-4f50-901e-f2b4631f2a15 \
  d9578038-9c13-4000-9a66-238db9450688 \
  56af6baa-3eeb-453e-8b16-138b9a876f23 \
  5957e6f0-d237-4797-aac9-eec3133bd5f4 \
  bdc4ec6a-76d3-4bff-8f9b-c15b7cd354d9 \
  05114aac-fbe8-49bd-856d-28a3db579c9e
do
  echo "Deleting $id"
  sudo /usr/local/bin/sbctl -d volume delete "$id" || exit $?
done
