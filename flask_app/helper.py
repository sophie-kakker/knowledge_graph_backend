def create_groups(group_str: str):
    str_groups = group_str.split(',')
    groups = []
    for gp in str_groups:
        groups.append(int(gp))
    return groups