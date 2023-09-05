
def log_results(f):
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        args[0].logger.info("%s -> %s", f.__name__, res)
        return res
    return wrapper


def validate_string(max_len=0):
    def decorator(f):
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            if max_len and len(res) <= max_len:
                args[0].logger.info("string variable is within the allowed length of VARCHAR(%s) ",  max_len)
            else:
                args[0].logger.error("string variable is NOT within the allowed length of VARCHAR(%s) ",  max_len)
                raise ValueError
            return res
        return wrapper
    return decorator
