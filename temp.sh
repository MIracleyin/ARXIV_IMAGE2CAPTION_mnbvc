Traceback (most recent call last):
  File "/Users/kaka/Coding/baidu/Codes/MNBVC_ARXIV_IMAGE2CAPTION/src/render.py", line 94, in <module>
    rendered_result = renderToHtml(formula)
  File "/Users/kaka/Coding/baidu/Codes/MNBVC_ARXIV_IMAGE2CAPTION/src/render.py", line 11, in renderToHtml
    result = ctx.call('blockKatexRender', formula_str)
  File "/Users/kaka/Library/Python/3.9/lib/python/site-packages/execjs/_abstract_runtime_context.py", line 37, in call
    return self._call(name, *args)
  File "/Users/kaka/Library/Python/3.9/lib/python/site-packages/execjs/_external_runtime.py", line 92, in _call
    return self._eval("{identifier}.apply(this, {args})".format(identifier=identifier, args=args))
  File "/Users/kaka/Library/Python/3.9/lib/python/site-packages/execjs/_external_runtime.py", line 78, in _eval
    return self.exec_(code)
  File "/Users/kaka/Library/Python/3.9/lib/python/site-packages/execjs/_abstract_runtime_context.py", line 18, in exec_
    return self._exec_(source)
  File "/Users/kaka/Library/Python/3.9/lib/python/site-packages/execjs/_external_runtime.py", line 88, in _exec_
    return self._extract_result(output)
  File "/Users/kaka/Library/Python/3.9/lib/python/site-packages/execjs/_external_runtime.py", line 167, in _extract_result
    raise ProgramError(value)
execjs._exceptions.ProgramError: TypeError: Cannot read properties of undefined (reading 'apply')