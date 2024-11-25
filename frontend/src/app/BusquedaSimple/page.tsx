"use client";

import React, { useState } from 'react';

// Define interface for the person data to match the full JSON structure
interface PersonaDesaparecida {
  id: number;
  fecha_extraccion: string;
  url_origen: string;
  fecha_modificacion: string;
  localizado: boolean;
  datos: {
    [key: string]: string | undefined;
    tez?: string;
    boca?: string;
    ojos?: string;
    peso?: string;
    sexo?: string;
    nombre?: string;
    cabello?: string;
    estatura?: string;
    registro?: string;
    complexion?: string;
    imagen_url?: string;
    tipo_nariz?: string;
    escolaridad?: string;
    tamano_nariz?: string;
    circunstancia?: string;
    originario_de?: string;
    fecha_nacimiento?: string;
    fecha_desaparicion?: string;
    senas_particulares?: string;
  };
}

// Utility function to remove accents and normalize text
function removeAccents(str: string): string {
  return str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
}

export default function BusquedaSimple() {
  const [nombre, setNombre] = useState('');
  const [apellidos, setApellidos] = useState('');
  const [resultados, setResultados] = useState<PersonaDesaparecida[]>([]);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async () => {
    const fullName = `${nombre} ${apellidos}`.trim();
    const encodedName = encodeURIComponent(removeAccents(fullName));

    try {
      const apiUrl = `http://localhost:3000/desaparecidos?datos->>nombre=ilike.*${encodedName}*`;
      
      const response = await fetch(apiUrl);
      
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      
      const data: PersonaDesaparecida[] = await response.json();
      setResultados(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? `Error al buscar: ${err.message}` : 'Error desconocido');
      setResultados([]);
    }
  };

  return (
    <div className="container">
      <div className="row mb-4">
        <div className="col-12">
          <h1 className="text-black font-mono">
            Búsqueda simple de personas desaparecidas
          </h1>
        </div>
      </div>
      
      <div className="row mb-4">
        <div className="col-12">
          <div className="bg-[#0C1F43] p-4 rounded">
            <h2 className="mb-3 font-mono">Por favor llena la siguiente información</h2>
            
            <div className="mb-3">
              <label className="form-label font-mono">Nombre de la persona desaparecida</label>
              <input
                type="text"
                className="form-control"
                placeholder="Ingresa el nombre"
                value={nombre}
                onChange={(e) => setNombre(e.target.value)}
              />
            </div>
            
            <div className="mb-3">
              <label className="form-label font-mono">Apellidos de persona desaparecida</label>
              <input
                type="text"
                className="form-control"
                placeholder="Ingresa el/los apellidos"
                value={apellidos}
                onChange={(e) => setApellidos(e.target.value)}
              />
            </div>
            
            <button
              onClick={handleSearch}
              className="btn btn-primary"
            >
              Buscar
            </button>
          </div>
        </div>
      </div>

      {error && (
        <div className="alert alert-danger">
          {error}
        </div>
      )}

      {resultados.length > 0 && (
        <div className="row">
          <div className="col-12">
            <h2>Resultados de la Búsqueda</h2>
            {resultados.map((persona) => (
              <div key={persona.id} className="card mb-3">
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
                      <h4>Detalles de la Inserción a la Base de Datos</h4>
                      <p><strong>Fecha de Extracción:</strong> {persona.fecha_extraccion}</p>
                      <p><strong>Fecha de Modificación:</strong> {persona.fecha_modificacion}</p>
                      <p><strong>URL de Origen:</strong> {persona.url_origen}</p>
                      <p><strong>Estado actual de la persona:</strong> {persona.localizado ? 'Localizada' : 'No Localizada'}</p>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}