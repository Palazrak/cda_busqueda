"use client"

import { useEffect } from "react"

const BootstrapClient = () => {
    useEffect(() => {
        void import("bootstrap/dist/js/bootstrap.bundle.min.js");
    }, [])

    return <></>;
}

export { BootstrapClient }
