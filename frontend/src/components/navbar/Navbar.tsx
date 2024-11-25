import Image from "next/image";
import logoAlertaAmber from "@/images/logoAmber.png"; 
import "./globals.css";

const Navbar = (props: any) => (
    <div className="container-fluid text-bg-white font-[family-name:var(--font-geist-mono)]">
        <a className="navbar-brand" href="/">
            <Image
                src={logoAlertaAmber}
                alt="logo"
                width={100}
                height={60}
                className="d-inline-block px-2 align-self-center"
            />
                <span className="font-bold text-2xl">Alertas Amber</span>
        </a>
    </div>
)