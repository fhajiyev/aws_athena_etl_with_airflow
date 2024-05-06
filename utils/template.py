
def apply_vars(m, ignore_keys, **vars):
    for k, v in m.items():
        if k in ignore_keys:
            continue
        elif isinstance(v, str):
            m[k] = v.format(**vars)
        elif isinstance(v, dict):
            apply_vars(v, ignore_keys, **vars)

def apply_jinja_template(string):
    return "{{ %s }}" % (string)
