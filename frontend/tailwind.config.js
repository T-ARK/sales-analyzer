/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        canvas: '#F8FAFC',
        surface: {
          1: '#FFFFFF',
          2: '#F1F5F9',
          3: '#E2E8F0',
        },
        brand: {
          indigo: '#4F46E5',
          cyan: '#0284C7',
          emerald: '#059669',
          amber: '#D97706',
          rose: '#DC2626'
        }
      },
      fontFamily: {
        geist: ['Inter', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      }
    },
  },
  plugins: [],
}
