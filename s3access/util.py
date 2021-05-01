def map_maybe(thing, func):
    if thing:
        return func(thing)
    return None
