import re
import sys


class PlacementDump:
    def __init__(self):
        self.data = {}

    def parse(self, file_path):
        """
        Parses the placement dump file and stores the data in the instance.

        Args:
            file_path (str): The path to the placement dump file.
        """
        with open(file_path, 'r') as file:
            current_lpgi = None
            current_layer = None

            for line in file:
                line = line.strip()

                # Match lines that contain "lpgi: <num>", optionally prefixed with geometry
                lpgi_pattern = re.search(r'^(.*?)(lpgi: (\d+))', line)
                if lpgi_pattern:
                    # We make a tuple key: (geometry_before_lpgi, lpgi_id)
                    # Example: for "mga n1 p1 lpgi: 79" -> ("mga n1 p1", 79)
                    #          for "lpgi: 79" -> ("", 79)
                    geometry = lpgi_pattern.group(1).strip() # e.g., "mga n1 p1"
                    lpgi_id = int(lpgi_pattern.group(3)) # e.g., 79
                    current_lpgi = (geometry, lpgi_id)
                    self.data[current_lpgi] = {'layers': [], 'columns': {}}
                    continue

                layer_match = re.match(r'layer: (\d+) (\d+) (\d+) (\d+) (\d+)', line)
                if layer_match:
                    current_layer = {
                        'ilayer': int(layer_match.group(1)),
                        'icolumn': int(layer_match.group(2)),
                        'storage_ID': int(layer_match.group(3)),
                        'valid': int(layer_match.group(4)),
                        'primary_flag': int(layer_match.group(5)),
                        'ranges': []
                    }
                    self.data[current_lpgi]['layers'].append(current_layer)
                    continue

                range_match = re.match(r'l: \[(\d+), (\d+)\)', line)
                if range_match and current_layer is not None:
                    current_layer['ranges'].append((int(range_match.group(1)), int(range_match.group(2))))
                    continue

                column_match = re.match(r'column: (\d+)', line)
                if column_match:
                    current_column = int(column_match.group(1))
                    self.data[current_lpgi]['columns'][current_column] = []
                    continue

                column_data_match = re.match(r'c: \[(\d+), (\d+)\) (\d+) (\d+) (\d+)', line)
                if column_data_match and current_lpgi is not None:
                    self.data[current_lpgi]['columns'][current_column].append({
                        'range': (int(column_data_match.group(1)), int(column_data_match.group(2))),
                        'res_loc': int(column_data_match.group(3)),
                        'storage_ID': int(column_data_match.group(4)),
                        'node_index': int(column_data_match.group(5))
                    })
                    continue

                # Unknown line. Placement dump broken?
                print("Unknown line in placement dump: {}".format(line))
                print("Unknown line in placement File: {}".format(file_path))
                exit(1)
                    

    def get_data(self):
        """
        Returns the parsed data.

        Returns:
            dict: The parsed data.
        """
        return self.data

    def equal(self, dst_placement):
        """
        Compares the layers and columns of this instance with another PlacementDump instance.
        Layer for specific storage_ID can have different ilayer number (for example after restart).
        That's why we don't compare maps directly.

        Args:
            dst_placement (PlacementDump): Another PlacementDump instance to compare with.

        Returns:
            bool: True - if the data is equal, 
                  False - otherwise.
        """
        if len(self.data) != len(dst_placement.data):
            return False

        for lpgi, entry in self.data.items():
            if lpgi not in dst_placement.data:
                return False

            layers1 = sorted(entry['layers'], key=lambda x: x['storage_ID'])
            layers2 = sorted(dst_placement.data[lpgi]['layers'], key=lambda x: x['storage_ID'])

            if len(layers1) != len(layers2):
                return False

            for layer1, layer2 in zip(layers1, layers2):
                if layer1['storage_ID'] != layer2['storage_ID'] or \
                        layer1['icolumn'] != layer2['icolumn'] or \
                        layer1['valid'] != layer2['valid'] or \
                        layer1['primary_flag'] != layer2['primary_flag'] or \
                        layer1['ranges'] != layer2['ranges']:
                    return False

            if entry['columns'] != dst_placement.data[lpgi]['columns']:
                return False

        return True

    def empty(self):
        """
        Checks if the placement map is empty.

        Returns:
            bool: True - if the placement map is empty, 
                  False - otherwise.
        """
        return len(self.data) == 0

    def check_after_failure_migration(self, dst_placement, storage_ID_failed):
        """
        Compares the map of this instance with another PlacementDump instance after failure migration.
        Layers and ranges should be the same, except for the ranges that were migrated.
        Columns and ranges should be the same, except for the ranges that were migrated.
        For the migrated ranges, the storage_ID should be different.

        Args:
            dst_placement: Another PlacementDump instance to compare with.
            storage_ID_failed: The storage_ID that failed and was migrated.

        Returns:
            bool: True - if the placement maps are equal after failure migration except for the migrated storage_ID,
                  False - otherwise.
        """
        for lpgi, entry in dst_placement.data.items():
            # Check if lpgi exists in both placement maps.
            if lpgi not in self.data:
                continue

            for layer in entry['layers']:
                if layer['storage_ID'] == storage_ID_failed:
                    print("Found failed storage_ID {} in lpgi {} in placement map".format(storage_ID_failed, lpgi))
                    return False

            # Compare columns and ranges on each column.
            # The columns should be equal, only the migrated storage_ID should be different.
            if self.data[lpgi]['columns'].keys() != entry['columns'].keys():
                return False
            for column, ranges in entry['columns'].items():
                for range_data in ranges:
                    if range_data['storage_ID'] == storage_ID_failed:
                        print("Found failed storage_ID {} in column {} in lpgi {} in placement map".format(storage_ID_failed, column, lpgi))
                        return False

        return True

    def check_garbage(self):
        """
        Checks if there is garbage in the placement map.
        If there is a layer that has ranges that are not in the columns or
        if the storage_ID for the column's range is not the same, then there is garbage.

        Returns:
            bool: True - if there is garbage in the placement map,
                  False - otherwise.
        """
        for lpgi, entry in self.data.items():
            for layer in entry['layers']:
                column = layer['icolumn']
                if column not in entry['columns']:
                    print("lpgi {}, layer {} contains garbage".format(lpgi, layer['ilayer']))
                    return True
                for range_layer in layer['ranges']:
                    matching_ranges = [r for r in entry['columns'][column] if r['range'] == range_layer]
                    if not matching_ranges:
                        print("lpgi {}, layer {} contains garbage".format(lpgi, layer['ilayer']))
                        return True
                    for range_data in matching_ranges:
                        if range_data['storage_ID'] != layer['storage_ID']:
                            print("lpgi {}, layer {} contains garbage".format(lpgi, layer['ilayer']))
                            return True
        return False

    def check_primary(self):
        """
        Check that all layers in the placement map are primary.

        Returns:
            bool: True - if all layers are primary,
                  False - otherwise.
        """
        for lpgi, entry in self.data.items():
            for layer in entry['layers']:
                if layer['primary_flag'] == 0:
                    print("lpgi {}, layer {} is not primary (storage_ID {}, icolumn {})".format(lpgi, layer['ilayer'], layer['storage_ID'], layer['icolumn']))
                    return False
        return True

    def check_columns_on_different_nodes(self):
        """
        Check that columns within each lpgi are stored on different nodes.

        Uses the node_index field from the placement dump to determine
        if any columns in the same lpgi share the same node.

        Returns:
            bool: True - if all columns are on different nodes for every lpgi,
                  False - otherwise.
        """
        for lpgi, entry in self.data.items():
            # Map each node_index to the columns that use it
            node_to_columns = {}
            for column, ranges in entry['columns'].items():
                for range in ranges:
                    node_idx = range['node_index']
                    node_to_columns.setdefault(node_idx, set()).add(column)
            
            # Check if any node has multiple columns
            for node_idx, columns in node_to_columns.items():
                if len(columns) > 1:
                    print("lpgi {}, columns {} share the same node (node_index {})".format(lpgi, sorted(columns), node_idx))
                    return False

        return True

    def check_columns_allocation_consistency(self):
        """
        Check that columns within each lpgi have valid allocation for 2+2 configuration.

        Rules for 2+2 (2 data chunks + 2 parity chunks):
        - Columns 2 and 3 (parity) must always have identical chunks
        - If column 0 or 1 contains a block, columns 2 and 3 must also contain it
        - Column 0 may not contain blocks from column 1 (and vice versa)

        Returns:
            bool: True - if allocation is consistent for every lpgi,
                  False - otherwise.
        """

        for lpgi, entry in self.data.items():
            columns = entry['columns']

            if len(columns) != 4:
                print("lpgi {} has {} columns, expected 4 for 2+2 configuration".format(lpgi, len(columns)))
                return False

            # Get allocated chunks for each column
            column_chunks = {}
            for column_id, ranges in columns.items():
                chunks = set()
                for r in ranges:
                    start, end = r['range']
                    for chunk in range(start, end):
                        chunks.add(chunk)
                column_chunks[column_id] = chunks

            chunks_0 = column_chunks.get(0, set())
            chunks_1 = column_chunks.get(1, set())
            chunks_2 = column_chunks.get(2, set())
            chunks_3 = column_chunks.get(3, set())

            # Rule 1: Columns 2 and 3 must be identical
            if chunks_2 != chunks_3:
                missing_in_3 = chunks_2 - chunks_3
                missing_in_2 = chunks_3 - chunks_2
                if missing_in_3:
                    missing_list = sorted(list(missing_in_3))[:20]
                    print("lpgi {}, chunks {} in column 2 but not in column 3{}".format(
                        lpgi, missing_list,
                        " ... ({} total)".format(len(missing_in_3)) if len(missing_in_3) > 20 else ""))
                if missing_in_2:
                    missing_list = sorted(list(missing_in_2))[:20]
                    print("lpgi {}, chunks {} in column 3 but not in column 2{}".format(
                        lpgi, missing_list,
                        " ... ({} total)".format(len(missing_in_2)) if len(missing_in_2) > 20 else ""))
                return False

            # Rule 2: If column 0 or 1 contains a block, columns 2 and 3 must also contain it
            data_chunks = chunks_0 | chunks_1
            missing_in_parity = data_chunks - chunks_2
            if missing_in_parity:
                missing_list = sorted(list(missing_in_parity))[:20]
                print("lpgi {}, chunks {} in data columns (0 or 1) but not in parity columns (2 and 3){}".format(
                    lpgi, missing_list,
                    " ... ({} total)".format(len(missing_in_parity)) if len(missing_in_parity) > 20 else ""))
                return False

            # Rule 3: Parity columns should not have extra chunks not in any data column
            extra_in_parity = chunks_2 - data_chunks
            if extra_in_parity:
                missing_list = sorted(list(extra_in_parity))[:20]
                print("lpgi {}, chunks {} in parity columns (2 and 3) but not in any data column (0 or 1){}".format(
                    lpgi, missing_list,
                    " ... ({} total)".format(len(extra_in_parity)) if len(extra_in_parity) > 20 else ""))
                return False

        return True

#
# Usage:
# 
# - Parse placement dump file:
# dump1 = PlacementDump()
# dump1.parse('/tmp/distrib_1_map_1741610984.txt')
#
# dump2 = PlacementDump()
# dump2.parse('/tmp/distrib_1_map_1741228163.txt')
#
#  - Compare two placement maps:
# print(dump1.equal(dump2))
#
# - Check that all layers in the placement map are primary:
# print(dump1.check_primary())
# 
# - Check if there is garbage in the placement map:
# print(dump1.check_garbage())
# 
# - Check that columns within each lpgi are stored on different nodes:
# print(dump1.check_columns_on_different_nodes())
#
# - Check that all columns have matching allocated chunks:
# print(dump1.check_columns_allocation_consistency())
#
# - Check that placement maps matches expected results after failure migration:
# print(dump1.check_after_failure_migration(dump2, 93))
#


# def main():
#     if len(sys.argv) != 2:
#         print("Usage: {} <placement_dump_file>".format(sys.argv[0]))
#         sys.exit(1)

#     file_path = sys.argv[1]

#     dump = PlacementDump()
#     try:
#         dump.parse(file_path)
#     except Exception as e:
#         print("FAIL: {} - parse error: {}".format(file_path, e))
#         sys.exit(1)

#     if dump.empty():
#         sys.exit(0)

#     if dump.check_columns_allocation_consistency():
#         sys.exit(0)
#     else:
#         print("FAIL: {}".format(file_path))
#         sys.exit(1)


# if __name__ == "__main__":
#     main()