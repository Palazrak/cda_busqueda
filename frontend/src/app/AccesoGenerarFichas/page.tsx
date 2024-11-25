import Image from "next/image";
import Link from 'next/link'
import { HomeIcon } from 'lucide-react'
import logoAlertaAmber from "@/images/logoAmber.png"

export default function AccesoGenerarFichas() {
    return (
        <div>
      <section
      >
        <div className="container">
          <div className="row">
            <div className="col-12 col-md-7">
              <p className="fs-1 fw-bold">
                <br></br>
                <span className="text-black font-[family-name:var(--font-geist-mono)]">
                  Acceso a portal para generar fichas de personas desaparecidas
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
                <br></br>Nombre del colectivo
              </span>
              <div className="col-6">
                <div className="input-group flex-nowrap">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa el nombre del colectivo"
                    aria-label="Ingresa el nombre del colectivo"
                    aria-describedby="addon-wrapping"
                  ></input>
                </div>
              </div>
              <span className="font-[family-name:var(--font-geist-mono)]">
                <br></br>Número de colectivo
              </span>
              <div className="col-6">
                <div className="input-group flex-nowrap">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa el número de colectivo"
                    aria-label="Ingresa el número de colectivo"
                    aria-describedby="addon-wrapping"
                  ></input>
                </div>
              </div>
              <span className="font-[family-name:var(--font-geist-mono)]">
                <br></br>Contraseña
              </span>
              <div className="col-6">
                <div className="input-group flex-nowrap">
                  <input
                    type="text"
                    className="form-control"
                    placeholder="Ingresa la contraseña"
                    aria-label="Ingresa la contraseña"
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
                  Ingresar<br></br>
                </a>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    );
}