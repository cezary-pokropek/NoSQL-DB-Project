def serializeDict(a) -> dict:
    try:
        result = {**{i:str(a[i]) for i in a if i=='_id'},**{i:a[i] for i in a if i!='_id'}}
    except TypeError: 
        result = []
    return result
