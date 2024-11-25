/** @type {import('next').NextConfig} */
const nextConfig = {
    output: 'standalone',
    eslint: {
        ignoreDuringBuilds: true,
    },
    images: {
        unoptimized: true  // Add this if you're still having image issues
    }
};

export default nextConfig;