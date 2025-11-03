# 📘 Understanding Context Variables in Jinja2

## 🧩 1. What Are Context Variables?

In **Jinja2**, *context variables* are the data values passed from your Python (or backend) code into the Jinja2 template.  
They define the **dynamic content** that the template can access and render.

> Think of a Jinja2 template as a blueprint, and context variables as the data used to fill in that blueprint.

---

## 🧠 2. How Context Variables Work

When rendering a template, you typically call something like:

```python
from jinja2 import Template

template = Template("Hello {{ user.name }}!")
rendered = template.render(user={"name": "Alex"})
print(rendered)
```

Output:
```
Hello Alex!
```

Here:
- `user` is a **context variable** (a Python object passed into the template).
- `user.name` is how you access its **property** inside the template.

---

## ⚙️ 3. Example with a Template Engine (e.g., Flask)

In a Flask app:
```python
@app.route('/profile')
def profile():
    return render_template('profile.html', username='Taylor', age=25)
```

In `profile.html`:
```jinja2
<p>Hello {{ username }}! You are {{ age }} years old.</p>
```

→ Flask automatically passes `username` and `age` into the template as **context variables**.

---

## 🧮 4. Context in Jinja2 Macros

Macros in Jinja2 can also use context variables — either passed explicitly or inherited from the parent template.

Example:
```jinja2
{% macro greet_user(name) %}
  <p>Hello, {{ name or user.name }}!</p>
{% endmacro %}
```

If the macro is called like this:

```jinja2
{{ greet_user() }}
```

and the global context has `user = {"name": "Sam"}`,  
then the output will be:
```
Hello, Sam!
```

---

## 🧱 5. Accessing Nested and Iterative Context Variables

You can access nested variables and loop through them easily:

```jinja2
{% for item in products %}
  <li>{{ item.name }} - ${{ item.price }}</li>
{% endfor %}
```

If `products` is a list of dictionaries, Jinja2 automatically resolves each item.

---

## 🔄 6. Filters and Expressions with Context Variables

You can apply **filters** directly to context variables:

```jinja2
{{ username | upper }}
{{ price | round(2) }}
{{ description | truncate(50) }}
```

Filters help you **transform or format** context data within templates.

---

## 🧰 7. Context Variable Precedence

If a variable is **defined both globally and locally**, Jinja2 will prioritize the **local variable** first.

```jinja2
{% set username = "Local" %}
<p>{{ username }}</p> {# → prints "Local" even if a global variable exists #}
```

---

## ⚠️ 8. Common Pitfalls

1. **Undefined Variables:**  
   If a variable is missing, Jinja2 will raise an error unless you handle it safely:  
   ```jinja2
   {{ user.name | default("Guest") }}
   ```

2. **Type Errors:**  
   Remember, context variables reflect Python types — e.g., lists, dicts, numbers, etc.

3. **Mutable Context:**  
   Be careful when modifying data directly in templates. It’s better to prepare clean context data in your backend.

---

## 📈 9. Debugging Context Variables

To inspect context variables inside a template:

```jinja2
<pre>{{ context | pprint }}</pre>
```

Or enable debugging in Flask / Jinja2 to trace missing variables.

---

## ✅ 10. Summary

| Concept | Description |
|----------|--------------|
| Context Variable | Data object passed from backend into template |
| Access | `{{ variable }}` or `{{ variable.attr }}` |
| Filters | Modify or format variable output |
| Scope | Local > Global |
| Common Use | Display dynamic text, lists, or conditionals |

---

**Author:** OpenAI ChatGPT  
**Topic:** Jinja2 Template Context Variables  
**Version:** v1.0  
**Date:** November 2025
