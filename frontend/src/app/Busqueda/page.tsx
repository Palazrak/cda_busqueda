import Image from "next/image";
import logoAlertaAmber from "@/images/logoAmber.png"

export default function Busqueda() {
   return(
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
    <div className="flex flex-col items-center justify-center text-center w-100 font-[family-name:var(--font-geist-mono)] mb-2">
        <div className="fw-bold text-center mb-4">
            Elige el tipo de búsqueda
        </div> 
        <div className="text-center">
            <span>Considera la siguiente información para la elección del tipo de búsqueda</span>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-8 w-full max-w-4xl px-4 mb-8">
        <div className="flex flex-col items-center">
        <h3 className="font-bold mb-4 mt-5">Búsqueda Simple</h3>
        <ul className="list-disc space-y-2">
            <li>Nombre completo</li>
            <li>Fecha de nacimiento</li>
        </ul>
        </div>

        <div className="flex flex-col items-center">
        <h3 className="font-bold mb-4 mt-5">Búsqueda Avanzada</h3>
        <ul className="list-disc space-y-2">
            <li>Nombre completo</li>
            <li>Fecha de nacimiento</li>
            <li>Fecha de desaparición</li>
            <li>Lugar de desaparición</li>
            <li>Imagen (opcional)</li>
        </ul>
        </div>
    </div>
    </div> 

    <div className="flex flex-col sm:flex-row items-center justify-center gap-4 w-full max-w-2xl">
        <a
          className="rounded-full border border-solid border-transparent transition-colors flex items-center justify-center bg-foreground text-background gap-2 hover:bg-[#383838] dark:hover:bg-[#ccc] text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5"
          href="BusquedaSimple"
        >
          Búsqueda simple
        </a>
        <a
          className="rounded-full border border-solid border-black/[.08] bg-[ffffff] transition-colors flex items-center justify-center hover:bg-[#ffffff] dark:hover:bg-[#1a1a1a] text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 sm:min-w-44"
          href="BusquedaAvanzada"
        >
          Búsqueda avanzada
        </a>
      </div>
    </main>
    
  </div>
   ); 
}