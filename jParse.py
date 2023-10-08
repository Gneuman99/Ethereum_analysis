import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        try:
            id = str(data["result"][k]["id"])
            category = data["result"][k]["category"]
            print(k+','+id+','+category, end='')
            print("")
        except:
            pass
