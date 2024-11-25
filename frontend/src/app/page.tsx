import Image from "next/image";
import Link from 'next/link'
import { HomeIcon } from 'lucide-react'
import logoAlertaAmber from "@/images/logoAmber.png"

export default function Home() {
  return (
    <div className="grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20 font-[family-name:var(--font-geist-sans)]">
      <main className="flex flex-col gap-8 row-start-2 items-center sm:items-start justify-items-center">
      <div className="d-flex justify-content-center w-100">
        <Image
          className="dark:invert"
          src={logoAlertaAmber.src}
          alt="Alerta Amber logo"
          width={200}
          height={50}
          priority
        />
      </div>
      <div className="d-flex justify-content-center text-center w-100 font-[family-name:var(--font-geist-mono)] mb-2">
        Escoge la opción que se adapte mejor a lo que buscas
      </div> 

        <div className="d-flex justify-content-center flex-col sm:flex-row">
          <a
            className="rounded-full border border-solid border-transparent transition-colors flex items-center justify-center bg-foreground text-background gap-2 hover:bg-[#383838] dark:hover:bg-[#ccc] text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5"
            href="Busqueda"
          >
            Búsqueda de persona
          </a>
          <a
            className="rounded-full border border-solid border-black/[.08] bg-[ffffff] transition-colors flex items-center justify-center hover:bg-[#ffffff] dark:hover:bg-[#1a1a1a] text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 sm:min-w-44"
            href="AccesoGenerarFichas"
          >
            Generar ficha persona desaparecida
          </a>
        </div>
      </main>
      <footer className="row-start-3 flex gap-6 flex-wrap items-center justify-center">
        <Link
           className="flex items-center gap-2 hover:underline hover:underline-offset-4"
            href="/"    
        >
          <HomeIcon size={16} aria-hidden = "true" />
            Go back to home page
        </Link>
      </footer>
    </div>
  );
}
