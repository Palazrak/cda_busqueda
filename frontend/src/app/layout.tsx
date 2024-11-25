import type { Metadata } from "next";
import localFont from "next/font/local";
import "../../node_modules/bootstrap/scss/bootstrap.scss";
import "./globals.css";
import { BootstrapClient } from "@/components/BootstrapLoader";

const geistSans = localFont({
  src: "./fonts/GeistVF.woff",
  variable: "--font-geist-sans",
  weight: "100 900",
});
const geistMono = localFont({
  src: "./fonts/GeistMonoVF.woff",
  variable: "--font-geist-mono",
  weight: "100 900",
});

export const metadata: Metadata = {
  title: "Alerta Amber",
  description: "Proyecto de centro de datos",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css" />
      </head>
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <BootstrapClient />
        {children}
      </body>
    </html>
  );
}
