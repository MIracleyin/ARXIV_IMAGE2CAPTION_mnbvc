import json
import execjs
from playwright.sync_api import sync_playwright
import os
# Compile the JavaScript code
ctx = execjs.compile(open(os.path.join(os.path.dirname(__file__),'formula.js')).read())

def renderToHtml(formula_str, render_type='katex'):
    # Call the JavaScript function with the provided formula
    if render_type == 'katex':
        result = ctx.call('blockKatexRender', formula_str)
    elif render_type == 'mathpix': # todo
        result = ctx.call('blockMathpixRender', formula_str)
    
    return json.loads(result)

def htmlToImage(html_str, output_path='output.png', padding=40):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        
        # Set the content of the page to the provided HTML string
        page.set_content(html_str)
        
        # Evaluate the dimensions of the content
        dimensions = page.evaluate("""
            () => {
                const element = document.querySelector('.latex-preview');
                const { width, height } = element.getBoundingClientRect();
                return { width, height };
            }
        """)
        
        # Set the viewport size to match the content dimensions with padding
        page.set_viewport_size({
            "width": int(dimensions['width']) + padding,  # Use the padding parameter
            "height": int(dimensions['height']) + padding  # Use the padding parameter
        })
        
        # Take a screenshot of the page
        page.screenshot(path=output_path, clip={
            "x": 0,
            "y": 0,
            "width": int(dimensions['width']) + padding,  # Use the padding parameter
            "height": int(dimensions['height']) + padding  # Use the padding parameter
        })
        
        browser.close()

def htmlTemplate(html_str):
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8"> 
        <style>
            body {{
                margin: 0;
                padding: 20px;
                background: white;
                font-size: 16px;
            }}
            .latex-preview {{
                display: inline-block;
            }}
        </style>
    </head>
    <body>
        <div class="latex-preview">
            {html_str}
        </div>
    </body>
    </html>
    """

def render_formula_fina(formula, output_filename):
    # Render the formula to HTML
    rendered_result = renderToHtml(formula)
    html_str = rendered_result["html"]
    
    # Create the full HTML document
    full_html = htmlTemplate(html_str)
    
    # Convert the rendered HTML to an image
    htmlToImage(full_html, output_filename, padding=30)
    print(f"Rendered {formula} to {output_filename}")

# Example usage
if __name__ == "__main__":
    formula = r"""
$$\begin{bmatrix}
a & b \\
c & d
\end{bmatrix}
\times
\begin{bmatrix}
e & f \\
g & h
\end{bmatrix}
=
\begin{bmatrix}
ae+bg & af+bh \\
ce+dg & cf+dh
\end{bmatrix}$$
    """
    formula = """$$\\begin{equation}\n\n\\label{E-2-2b}\n\n\\vec{x}_i \\rightarrow \\vec{x}_i + (\\alpha - 1) \\\n\n\\vec{e}_{\\alpha}\\cdot\\vec{x}_i \\ \\vec{e}_{\\alpha}\n\n\\ .\n\n\\end{equation}$$"""
    rendered_result = renderToHtml(formula)
    html_str = rendered_result["html"]
    print(html_str)
    full_html = htmlTemplate(html_str)
    # Convert the rendered HTML to an image
    htmlToImage(full_html, 'formula_image.png', padding=20)