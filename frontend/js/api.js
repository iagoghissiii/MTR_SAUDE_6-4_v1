/**
 * Modulo de comunicacao com a API FastAPI (TCC v1.0).
 */

const API_BASE = "http://localhost:8000/api/v1";


async function apiRequest(endpoint, options = {}) {
    const url = `${API_BASE}${endpoint}`;
    const config = {
        headers: { "Content-Type": "application/json" },
        ...options,
    };

    const response = await fetch(url, config);

    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(error.detail || `Erro ${response.status}`);
    }

    return response.json();
}


async function fetchContratos(filtros = {}) {
    const params = new URLSearchParams();
    if (filtros.valorMin) params.append("valor_min", filtros.valorMin);
    if (filtros.valorMax) params.append("valor_max", filtros.valorMax);
    if (filtros.ordem) params.append("ordem", filtros.ordem);
    if (filtros.pagina) params.append("pagina", filtros.pagina);
    if (filtros.limite) params.append("limite", filtros.limite);

    return apiRequest(`/contratos?${params}`);
}

async function fetchContratoDetalhe(id) {
    return apiRequest(`/contratos/${id}`);
}

async function fetchDashboardResumo() {
    return apiRequest("/contratos/dashboard");
}
