import Document, { Html, Head, Main, NextScript, DocumentContext } from 'next/document';

// Minimal MUI + Next.js Pages Router Document.
// For SSR styles via Emotion, we can add extraction later if needed.

export default class MyDocument extends Document {
  static async getInitialProps(ctx: DocumentContext) {
    const initialProps = await Document.getInitialProps(ctx);
    return { ...initialProps };
  }

  render() {
    return (
      <Html lang="en">
        <Head />
        <body>
          <Main />
          <NextScript />
        </body>
      </Html>
    );
  }
}
