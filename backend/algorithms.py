def manual_group_count_route(pairs):
    counts = {}
    for pu, do in pairs:
        key = f"{pu}->{do}"
        if key in counts:
            counts[key] += 1
        else:
            counts[key] = 1
    items = []
    for k in counts:
        items.append((k, counts[k]))
    return items

def merge_sort_desc(items):
    if len(items) <= 1:
        return items
    mid = len(items) // 2
    left = merge_sort_desc(items[:mid])
    right = merge_sort_desc(items[mid:])
    return _merge(left, right)

def _merge(left, right):
    out = []
    i = 0
    j = 0
    while i < len(left) and j < len(right):
        if left[i][1] >= right[j][1]:
            out.append(left[i])
            i += 1
        else:
            out.append(right[j])
            j += 1
    while i < len(left):
        out.append(left[i])
        i += 1
    while j < len(right):
        out.append(right[j])
        j += 1
    return out

def top_k_routes_manual(pairs, k):
    grouped = manual_group_count_route(pairs)
    ranked = merge_sort_desc(grouped)
    result = []
    idx = 0
    while idx < len(ranked) and idx < k:
        route, count = ranked[idx]
        pu, do = route.split("->")
        result.append({"pu_location_id": int(pu), "do_location_id": int(do), "trip_count": count})
        idx += 1
    return result
