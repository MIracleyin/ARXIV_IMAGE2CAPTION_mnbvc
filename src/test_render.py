from render import renderToHtml, htmlTemplate, htmlToImage

def test_render_formula(formula, output_filename):
    # Render the formula to HTML
    rendered_result = renderToHtml(formula)
    html_str = rendered_result["html"]
    
    # Create the full HTML document
    full_html = htmlTemplate(html_str)
    
    # Convert the rendered HTML to an image
    htmlToImage(full_html, output_filename, padding=30)
    print(f"Rendered {formula} to {output_filename}")

def main():
    # List of test formulas
    test_formulas = [
        (r'$$ \int_{a}^{b} x^2 \, dx = \frac{b^3}{3} - \frac{a^3}{3} $$', 'demo/integral.png'),
        (r'$$ \begin{bmatrix} a & b \\ c & d \end{bmatrix} \times \begin{bmatrix} e & f \\ g & h \end{bmatrix} = \begin{bmatrix} ae+bg & af+bh \\ ce+dg & cf+dh \end{bmatrix} $$', 'demo/matrix.png'),
        (r'$$ \lim_{x \to \infty} \left(1 + \frac{1}{x}\right)^x = e $$', 'demo/limit.png'),
        (r'$$ \frac{d}{dx}\left( e^{x^2} \right) = 2xe^{x^2} $$', 'demo/differential.png'),
        (r'$$ \hat{f}(\xi) = \int_{-\infty}^{\infty} f(x) e^{-2\pi i x \xi} \, dx $$', 'demo/fourier.png'),
        (r'$$ \delta(x) = \begin{cases} \infty, & x = 0 \\ 0, & x \neq 0 \end{cases} \quad \text{and} \quad \int_{-\infty}^{\infty} \delta(x) \, dx = 1 $$', 'demo/dirac.png'),
        (r'$$ P(A|B) = \frac{P(B|A)P(A)}{P(B)} $$', 'demo/bayes.png')
    ]

    # Run tests
    for formula, filename in test_formulas:
        test_render_formula(formula, filename)

if __name__ == "__main__":
    main() 