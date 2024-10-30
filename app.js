const apiKey = 'sua api aqui'; 
const inputImagem = document.getElementById('inputImagem');
const loading = document.getElementById('loading');
const error = document.getElementById('error');
const resultado = document.getElementById('resultado');

inputImagem.addEventListener('change', analisarImagem);

async function analisarImagem(event) {
    const file = event.target.files[0];
    if (!file) return;

  
    const imagemSelecionada = document.getElementById('imagemSelecionada');
    const reader = new FileReader();
    reader.onload = () => {
        imagemSelecionada.src = reader.result;
        imagemSelecionada.style.display = 'block'; 
    };
    reader.readAsDataURL(file);

    loading.style.display = 'block';
    error.style.display = 'none';
    resultado.style.display = 'none';

    try {
        const base64Image = await converterParaBase64(file);
        const requestBody = {
            contents: [{
                parts: [{
                    text: `Analise esta imagem facial e forneça os seguintes detalhes em formato JSON:
                           {
                             "genero": "<gênero>",
                             "etnia": "<etnia>",
                             "formato_rosto": "<formato do rosto>",
                             "melhor_armacao": "<tipo de armação recomendada>"
                           }`
                }, {
                    inline_data: {
                        mime_type: file.type,
                        data: base64Image
                    }
                }]
            }],
            generationConfig: {
                temperature: 0.4,
                topK: 32,
                topP: 1,
                maxOutputTokens: 4096,
            }
        };

        const response = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=${apiKey}`,
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody)
            }
        );

        if (!response.ok) {
            throw new Error('Falha ao analisar a imagem');
        }

        const data = await response.json();
        const result = JSON.parse(data.candidates[0].content.parts[0].text);
        exibirResultados(result);
    } catch (error) {
        console.error('Erro:', error);
        error.textContent = 'Falha ao analisar a imagem. Por favor, tente novamente.';
        error.style.display = 'block';
    } finally {
        loading.style.display = 'none';
    }
}

function converterParaBase64(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result.split(',')[1]);
        reader.onerror = reject;
        reader.readAsDataURL(file);
    });
}

function exibirResultados(result) {
    document.getElementById('genero').textContent = result.genero;
    document.getElementById('etnia').textContent = result.etnia;
    document.getElementById('formatoRosto').textContent = result.formato_rosto;
    document.getElementById('melhorArmacao').textContent = result.melhor_armacao;
    resultado.style.display = 'block';
}
