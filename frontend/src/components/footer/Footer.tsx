import Link from 'next/link'
import { HomeIcon } from 'lucide-react'

const Footer = (props: any) => {
    return (
        <footer className="row-start-3 flex gap-6 flex-wrap items-center justify-center">
        <Link
           className="flex items-center gap-2 hover:underline hover:underline-offset-4"
            href="/"    
        >
          <HomeIcon size={16} aria-hidden = "true" />
            Go back to home page
        </Link>
      </footer>
    ); 
}