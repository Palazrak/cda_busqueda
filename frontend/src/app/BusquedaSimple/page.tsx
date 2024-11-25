export default function BusquedaSimple() {
   return(
    <div>
    <section
    >
      <div className="container">
        <div className="row">
          <div className="col-12 col-md-7">
            <p className="fs-1 fw-bold">
              <br></br>
              <span className="text-black font-[family-name:var(--font-geist-mono)]">
                Búsqueda simple de personas desaparecidas
              </span>
            </p>
          </div>
        </div>
      </div>
    </section>
    <div className="col-12">
      <div className="container bg-[F5E0BC] rounded">
        <div className="row">
          <div className="col-12 col-md-8 offset-md-2"></div>
         
          <span className="text-black fw-bold fs-5 font-[family-name:var(--font-geist-mono)]">
            <br></br>Por favor llena la siguiente información
          </span>

          <div className="row">
            <span className="font-[family-name:var(--font-geist-mono)]">
              <br></br>Nombre de la persona desaparecida
            </span>
            <div className="col-6">
              <div className="input-group flex-nowrap">
                <input
                  type="text"
                  className="form-control"
                  placeholder="Ingresa el nombre"
                  aria-label="Ingresa el nombre"
                  aria-describedby="addon-wrapping"
                ></input>
              </div>
            </div>
            <span className="font-[family-name:var(--font-geist-mono)]">
              <br></br>Apellidos de persona desaparecida
            </span>
            <div className="col-6">
              <div className="input-group flex-nowrap">
                <input
                  type="text"
                  className="form-control"
                  placeholder="Ingresa el/los apellidos"
                  aria-label="Ingresa el/los apellidos"
                  aria-describedby="addon-wrapping"
                ></input>
              </div>
            </div>
            <div className="col-6 offset-4">
              <a
                href="/"
                className="btn bg-[#DE5F07] text-black fs-6 fw-bold ms-3 mt-4 text-center border-black"
                type="button"
              >
                Buscar<br></br>
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

   ); 
}