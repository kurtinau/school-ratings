def convert2Dict(lst):
    dict = {}
    for i in range(0, len(lst), 2):
        key = str(lst[i]).strip().lower().replace(' ', '')
        value = str(lst[i+1]).strip().lower()[1:].replace(',', '')
        dict[key] = value
    return dict
