package com.google.blockToBq;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.FilteredBlock;
import org.bitcoinj.core.FullPrunedBlockChain;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.core.listeners.DownloadProgressTracker;
import org.bitcoinj.net.discovery.DnsDiscovery;
import org.bitcoinj.net.discovery.SeedPeers;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.LevelDBFullPrunedBlockStore;

public class BitcoinBlockDownloader {
  /** Name used to identify ourselves to blockchain network. */
  public static final String AGENT_NAME = "BlockchainToBq";

  /** Version of our downloader. */
  public  static final String AGENT_VERSION = "1.0";

  /** Max number of peers to connect to. */
  public static final int MAX_CONNECTIONS = 1000;

  /** How long to wait before timing out a peer. */
  public static final int CONNECTION_TIMEOUT_MILLIS = 5000;

  private AtomicBoolean isDone;
  private PeerGroup peerGroup;
  private String dblocation;

  /** Instantiate a new BitcoinBlockDownloader instance. */
  public BitcoinBlockDownloader(String dblocation) {
    this.dblocation = dblocation;
    if (!this.dblocation.endsWith("/")) {
      this.dblocation += "/";
    }
    isDone = new AtomicBoolean(false);

  }

  /** Blocks until the download of the blockchain is stopped, and peers disconnected. */
  public void stop() {
    if (peerGroup != null) {
      peerGroup.stop();
      peerGroup = null;
    }
    isDone.set(false);
  }

  /** Starts the process of downloading the latest blocks stored in the blockchain. */
  public void start(NetworkParameters networkParameters, BlockListener blockListener) throws BlockStoreException {
    stop();

    // Connect to Bitcoin MainNet
    Context.getOrCreate(networkParameters);

    // Validate the chain we're downloading, vs assuming incoming blocks are valid
    LevelDBFullPrunedBlockStore blockStore = new LevelDBFullPrunedBlockStore(
        networkParameters, this.dblocation, Integer.MAX_VALUE);
    FullPrunedBlockChain blockChain = new FullPrunedBlockChain(networkParameters, blockStore);

    // configure what peers we connect to
    peerGroup = new PeerGroup(networkParameters, blockChain);
    peerGroup.setUserAgent(AGENT_NAME, AGENT_VERSION);
    peerGroup.addPeerDiscovery(new DnsDiscovery(networkParameters));
    peerGroup.addPeerDiscovery(new SeedPeers(networkParameters));
    peerGroup.setUserAgent(AGENT_NAME, AGENT_VERSION);
    peerGroup.setMaxConnections(MAX_CONNECTIONS);
    peerGroup.setConnectTimeoutMillis(CONNECTION_TIMEOUT_MILLIS);

    // attach listener for newly downloaded blockchain blocks
    peerGroup.addBlocksDownloadedEventListener((Peer peer, Block block,
        @Nullable FilteredBlock filteredBlock, int i) -> {
      long blockHeight = peer.getBestHeight() - i;
      blockListener.onBlock(blockHeight, block);
    });

    // download the chain
    peerGroup.start();
    peerGroup.startBlockChainDownload(new DownloadProgressTracker() {
      protected void doneDownload() {
        super.doneDownload();
        isDone.set(true);
      }
    });
  }

  /** Interface for callback used in {@link #start(NetworkParameters, BlockListener)}. */
  public interface BlockListener {
    void onBlock(long blockHeight, Block block);
  }

  /** Informs you if blockchain download is finished. */
  public boolean isDone() {
    return isDone.get();
  }
}
