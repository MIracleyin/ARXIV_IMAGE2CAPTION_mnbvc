var katexmd = require('markdown-it')();
katexmd.use(require('markdown-it-katex').default);

function blockKatexRender(text) {
    const result = katexmd.render(text);
    const hasError = result.includes('katex-error') || result.includes('ParseError');

    if (!hasError) {
        // 提取 <math> 部分
        const mathOnly = result.match(/<math[\s\S]*?<\/math>/)[0];

        return JSON.stringify({
            success: true,
            html: mathOnly,
            raw: text,
        });
    } else {
        return JSON.stringify({
            success: false,
            html: result,
            raw: text,
        });
    }
}

// function blockMathpixRender(text) {
//     const result = mathpixmd.render(text);
//     return JSON.stringify({
//         success: true,
//         html: result,   
//         raw: text,
//     });
// }

// console.log(blockRender("e=mc^2"));

