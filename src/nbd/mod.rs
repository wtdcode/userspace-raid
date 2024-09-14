pub mod client;
pub mod kernel;
pub mod proto;
pub mod server;

#[cfg(test)]
mod tests {
    use color_eyre::Result;
    use readwrite::{ReadWrite, ReadWriteTokio};
    use std::io::prelude::*;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::task::JoinHandle;

    use crate::nbd::server::MemBlocks;
    use crate::nbd::{client::Client, server::Server};

    struct ServerClient<IO: AsyncRead + AsyncWrite + Unpin> {
        server: JoinHandle<Result<()>>,
        client: Client<IO>,
    }

    impl<IO: AsyncRead + AsyncWrite + Unpin> ServerClient<IO> {
        async fn shutdown(self) -> Result<()> {
            self.client.disconnect().await?;
            self.server.await.unwrap()?;
            Ok(())
        }
    }

    async fn start_server_client(
        data: Vec<u8>,
    ) -> Result<ServerClient<impl AsyncRead + AsyncWrite>> {
        // let _ = env_logger::builder().is_test(true).try_init();
        let (r1, w1) = tokio_pipe::pipe()?;
        let (r2, w2) = tokio_pipe::pipe()?;
        let s1 = ReadWriteTokio::new(r1, w2);
        let s2 = ReadWriteTokio::new(r2, w1);

        let s_handle = tokio::spawn(async move {
            let server = Server::new(MemBlocks::new(data));
            server.handle_client(s1).await?;
            Ok(())
        });

        let client = Client::new(s2).await?;

        Ok(ServerClient {
            server: s_handle,
            client,
        })
    }

    #[tokio::test]
    async fn run_client_server_handshake() -> Result<()> {
        let data = vec![1u8; 1024 * 10];
        let sc = start_server_client(data).await?;

        sc.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn client_hard_disconnect() -> Result<()> {
        let data = vec![1u8; 1024 * 10];
        let ServerClient { server, client } = start_server_client(data).await?;

        // we don't call disconnect on client, but drop it to close the connection
        drop(client);
        // server should not error in this situation
        server.await.unwrap()?;

        Ok(())
    }

    #[tokio::test]
    async fn client_export_size() -> Result<()> {
        let len = 15341;
        let data = vec![1u8; len];
        let sc = start_server_client(data).await?;

        assert_eq!(sc.client.size(), len as u64);

        sc.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn run_client_server_read_write() -> Result<()> {
        let data = vec![1u8; 1024 * 10];
        let mut sc = start_server_client(data).await?;
        let client = &mut sc.client;

        let buf = client.read(3, 5).await?;
        assert_eq!(buf, [1u8; 5]);
        client.write(4, &[9u8; 7]).await?;
        client.flush().await?;
        let buf = client.read(2, 4).await?;
        assert_eq!(buf, [1, 1, 9, 9]);

        sc.shutdown().await?;
        Ok(())
    }
}
