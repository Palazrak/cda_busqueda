'use client';

import { useState } from 'react';

// Definir el tipo de dato esperado
interface PersonaDesaparecida {
  id: number;
  fecha_extraccion: string;
  url_origen: string;
  fecha_modificacion: string;
  localizado: boolean;
  datos: {
    [key: string]: string | undefined | null;
    nombre?: string;
    imagen_url?: string;
    fecha_nacimiento?: string;
    fecha_hechos?: string;
    lugar_hechos?: string;
    senas_particulares?: string;
    estatura?: string;
    peso?: string;
    genero?: string;
    estado?: string;
    resumen_hechos?: string;
  };
}

// Función utilitaria para limpiar espacios de más
function normalizarTexto(texto: string): string {
  return texto.trim().replace(/\s+/g, ' ');
}

export default function BusquedaAvanzada() {
  const [nombre, setNombre] = useState('');
  const [apellidos, setApellidos] = useState('');
  const [foto, setFoto] = useState<File | null>(null);
  const [resultados, setResultados] = useState<PersonaDesaparecida[]>([]);
  const [error, setError] = useState<string | null>(null);

  const handleOnChangeFoto = (event: any) => {
    const file = event.target.files?.[0];
    if (file) {
      setFoto(file);
    }
  };

  const handleBusquedaAvanzada = async () => {
    const nombreLimpio = normalizarTexto(nombre);
    const apellidosLimpios = normalizarTexto(apellidos);

    if (!nombreLimpio) {
      setError('Por favor ingresa al menos el nombre.');
      return;
    }
    if (!foto) {
      setError('Por favor sube una foto para hacer la búsqueda.');
      return;
    }

    setError(null);

    try {
      const formData = new FormData();
      formData.append('nombre', nombreLimpio);
      if (apellidosLimpios) {
        formData.append('apellidos', apellidosLimpios);
      }
      formData.append('foto', foto);

      const response = await fetch('http://localhost:8000/busqueda-avanzada', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Error al realizar la búsqueda.');
      }

      const data = await response.json();
      setResultados(data.resultados || []);
    } catch (err: any) {
      setError(err.message || 'Error desconocido.');
      setResultados([]);
    }
  };

  return (
    <div className="container">
      <section>
        <div className="row">
          <div className="col-12 col-md-7">
            <p className="fs-1 fw-bold">
              <br />
              <span className="text-black font-[family-name:var(--font-geist-mono)]">
                Búsqueda avanzada de personas desaparecidas
              </span>
            </p>
          </div>
        </div>
      </section>

      <div className="col-12">
        <div className="container bg-[F5E0BC] rounded p-4">
          <span className="text-black fw-bold fs-5 font-[family-name:var(--font-geist-mono)] d-block mb-4">
            Por favor llena la siguiente información
          </span>

          <div className="row">
            {/* Primera columna */}
            <div className="col-md-6">
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Nombre de la persona desaparecida
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa el nombre"
                    value={nombre}
                    onChange={(e) => setNombre(e.target.value)}
                  />
                </div>
              </div>

              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Fecha de nacimiento
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="(Opcional)"
                    disabled
                  />
                </div>
              </div>

              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Lugar de desaparición
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="(Opcional)"
                    disabled
                  />
                </div>
              </div>
            </div>

            {/* Segunda columna */}
            <div className="col-md-6">
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Apellidos de la persona desaparecida
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa el/los apellidos"
                    value={apellidos}
                    onChange={(e) => setApellidos(e.target.value)}
                  />
                </div>
              </div>

              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Fecha de desaparición
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="(Opcional)"
                    disabled
                  />
                </div>
              </div>

              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Foto de desaparecido
                </span>
                <div className="input-group">
                  <input
                    className="form-control"
                    type="file"
                    id="foto"
                    aria-label="Foto"
                    onChange={handleOnChangeFoto}
                  />
                </div>
              </div>
            </div>
          </div>

          {/* Botón de búsqueda */}
          <div className="row">
            <div className="col-12 text-center mt-4">
              <button
                onClick={handleBusquedaAvanzada}
                className="btn bg-[#DE5F07] text-black fs-6 fw-bold border-black px-4"
                type="button"
              >
                Buscar
              </button>
            </div>
          </div>

          {/* Resultado de la búsqueda */}
          {error && (
            <div className="alert alert-danger mt-4">{error}</div>
          )}

          {resultados.length > 0 && (
            <div className="row mt-5">
              <div className="col-12">
                <h2>Resultados de la búsqueda</h2>
                {resultados.map((persona) => (
                  <div key={persona.id} className="card mb-4">
                    <div className="card-header">
                      <h3>{persona.datos.nombre || 'Nombre no disponible'}</h3>
                    </div>
                    <div className="card-body">
                      <div className="row">
                        {persona.datos.imagen_url && (
                          <div className="col-md-4">
                            <img
                              src={persona.datos.imagen_url}
                              alt="Fotografía de la persona"
                              className="img-fluid rounded"
                            />
                          </div>
                        )}
                        <div className="col-md-8">
                          <h4>Información Personal</h4>
                          {Object.entries(persona.datos).map(([key, value]) => (
                            value && (
                              <p key={key}>
                                <strong>{key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}:</strong> {value}
                              </p>
                            )
                          ))}
                          <hr />
                          <h4>Detalles en la Base de Datos</h4>
                          <p><strong>Fecha de Extracción:</strong> {persona.fecha_extraccion}</p>
                          <p><strong>Fecha de Modificación:</strong> {persona.fecha_modificacion}</p>
                          <p><strong>URL de Origen:</strong> {persona.url_origen}</p>
                          <p><strong>Estado actual de la persona:</strong> {persona.localizado ? 'Localizada' : 'No localizada'}</p>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
