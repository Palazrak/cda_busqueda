import { useState } from "react";
//import { FileInterface } from "../api/send/jobapplication/route";

export default function BusquedaAvanzada() {
    
    const handleOnChangeResume = (event: any) => {
        //const [resume, setResume] = useState<FileInterface[]>([])

        //event.preventDefault();
        //const files = event.target.files;
        //if (files) {
        //Promise.all(
            //Array.from(files).map(async (file: any) => ({
            //fileName: file.name,
            //content: Buffer.from(await file.arrayBuffer()).toString('base64'),
            //}))
        //).then((filesArray) => setResume(filesArray))
        //}
    }
return (
    <div>
      <section>
        <div className="container">
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
        </div>
      </section>

      <div className="col-12">
        <div className="container bg-[F5E0BC] rounded p-4">
          <span className="text-black fw-bold fs-5 font-[family-name:var(--font-geist-mono)] d-block mb-4">
            Por favor llena la siguiente información
          </span>

          <div className="row">
            {/* First Column */}
            <div className="col-md-6">
              {/* Element 1 */}
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Nombre de la persona desaparecida
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa el nombre"
                    aria-label="Ingresa el nombre"
                  />
                </div>
              </div>

              {/* Element 2 */}
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Fecha de nacimiento
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa fecha de nacimiento"
                    aria-label="Ingresa fecha de nacimiento"
                  />
                </div>
              </div>

              {/* Element 3 */}
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Lugar de desaparición
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa lugar de desaparición"
                    aria-label="Ingresa lugar de desaparición"
                  />
                </div>
              </div>
            </div>

            {/* Second Column */}
            <div className="col-md-6">
              {/* Element 4 */}
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Apellidos de persona desaparecida
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa el/los apellidos"
                    aria-label="Ingresa el/los apellidos"
                  />
                </div>
              </div>

              {/* Element 5 */}
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Fecha de desaparición
                </span>
                <div className="input-group">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa fecha de desaparición"
                    aria-label="Ingresa fecha de desaparición"
                  />
                </div>
              </div>

              {/* Element 6 */}
              <div className="mb-4">
                <span className="font-[family-name:var(--font-geist-mono)] d-block mb-2">
                  Foto de desaparecido
                </span>
                <div className="input-group">
                  <input
                    className="form-control"
                    type="file"
                    id="resume"
                    placeholder="Attach resume"
                    aria-label="Resume"
                    //qwqeonChange={handleOnChangeResume}
                  />
                </div>
              </div>
            </div>
          </div>

          {/* Search Button */}
          <div className="row">
            <div className="col-12 text-center mt-4">
              <a
                href="/"
                className="btn bg-[#DE5F07] text-black fs-6 fw-bold border-black px-4"
                type="button"
              >
                Buscar
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
 
}