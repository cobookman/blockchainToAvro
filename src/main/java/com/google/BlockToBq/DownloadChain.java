package com.google.BlockToBq;

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
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.LevelDBFullPrunedBlockStore;

public class DownloadChain {
  private AtomicBoolean isDone;
  private PeerGroup peerGroup;
  private static final String agentName = "BlockchainToBq";
  private static final String agentVersion = "1.0-beta";

  public DownloadChain() {
    isDone = new AtomicBoolean(false);
  }

  public void start(BlockListener blockListener) throws BlockStoreException, InterruptedException {
    NetworkParameters networkParameters = new MainNetParams();
    Context.getOrCreate(networkParameters);

    LevelDBFullPrunedBlockStore blockStore = new LevelDBFullPrunedBlockStore(networkParameters, ".data/",
        Integer.MAX_VALUE);
    FullPrunedBlockChain blockChain = new FullPrunedBlockChain(networkParameters, blockStore);

    // add peers for downloading block chain
    peerGroup = new PeerGroup(networkParameters, blockChain);
    peerGroup.setUserAgent(agentName, agentVersion);
    peerGroup.addPeerDiscovery(new DnsDiscovery(networkParameters));
    peerGroup.addPeerDiscovery(new SeedPeers(networkParameters));
    peerGroup.start();
    peerGroup.startBlockChainDownload(new DownloadProgressTracker() {
      protected void doneDownload() {
        super.doneDownload();
        isDone.set(true);
      }
    });

    peerGroup.addBlocksDownloadedEventListener((Peer peer, Block block,
        @Nullable FilteredBlock filteredBlock, int i) -> {
      blockListener.onBlock(block);
    });
  }

  public interface BlockListener {
    void onBlock(Block block);
  }

  public void blockTillDone() {
    peerGroup.downloadBlockChain();
  }
}
