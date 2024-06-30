def convert_list_of_dicts_to_dict(list_of_dicts):
    result = {}
    for d in list_of_dicts:
        for key, value in d.items():
            if key in result:
                result[key].append(value)
            else:
                result[key] = [value]
    return result
