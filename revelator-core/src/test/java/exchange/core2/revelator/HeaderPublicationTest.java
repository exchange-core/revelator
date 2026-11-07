package exchange.core2.revelator;

import exchange.core2.revelator.fences.*;
import exchange.core2.revelator.processors.*;
import exchange.core2.revelator.processors.simple.*;
import org.junit.Test;
import exchange.core2.revelator.processors.pipelined.*;
import static org.junit.Assert.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class HeaderPublicationTest {
    @Test(timeout=10000) public void variableFramesAndWrap() {
        for (FrameLayout layout : List.of(FrameLayout.packed(), FrameLayout.lineAligned())) {
            long[] seen={0};
            try (var r=Revelator.create(1024, ProcessorsFactories.single((b,i,n,t,p,id,k)->{
                assertEquals(seen[0],b[i]);
                for(int j=1;j<n;j++)assertEquals(b[i]^j,b[i+j]);
                seen[0]++;
            }), Thread::new, Revelator.PublicationMode.HEADER_COMMIT,layout)) {
                r.start();
                for(int v=0;v<10000;v++) {
                    int n=1+(v*97)%500;
                    long p=r.claimSingleMessage(n,0,v,(byte)1);
                    for(int j=0;j<n;j++)r.writeLongData(p,j,j==0?v:v^j);
                    r.publish(p+n);
                }
            }
            assertEquals(10000,seen[0]);
        }
    }
    @Test(timeout=10000) public void batchReverseAndPrefix() {
        for(var mode:Revelator.PublicationMode.values()) {
            AtomicInteger count=new AtomicInteger();
            try(var r=Revelator.create(4096,ProcessorsFactories.single((b,i,n,t,p,id,k)->count.incrementAndGet()),Thread::new,mode)) {
                r.start();
                for(int batch=0;batch<100;batch++) {
                    long first=0,end=0;
                    for(int i=0;i<16;i++) {long p=r.claimSingleMessage(1,0,1,(byte)1);r.writeLongData(p,0,i);end=p+1;if(i==3)first=end;}
                    r.publish(first);r.publish(end);
                }
            }
            assertEquals(1600,count.get());
        }
    }
    @Test(timeout=5000) public void adversarialOldPayloadCannotImpersonateSuccessor() throws Exception {
        IFence[] input=new IFence[1];SingleWriterFence released=new SingleWriterFence();
        AtomicReference<Thread> worker=new AtomicReference<>();
        IFlowProcessor parked=new IFlowProcessor(){
            public void run(){while(!Thread.currentThread().isInterrupted())Thread.onSpinWait();}
            public SingleWriterFence getReleasingFence(){return released;}
        };
        var r=Revelator.create(1024,(f,c)->{input[0]=f;return new IFlowProcessorsFactory.ProcessorsChain(List.of(parked),released);},
            job->{Thread t=new Thread(job);worker.set(t);return t;},Revelator.PublicationMode.HEADER_COMMIT);
        try {
            r.start();long p=r.claimSingleMessage(1004,0,1,(byte)1);
            r.writeLongData(p,0,1L<<56);r.writeLongData(p,1,44);r.writeLongData(p,2,0);
            r.writeLongData(p,3,(1028L+1)^0x5EED_C0DE_D1CE_F00DL);
            r.publish(p+1004);released.setRelease(p+1004);
            for(int i=0;i<5;i++){p=r.claimSingleMessage(0,0,1,(byte)1);r.publish(p);released.setRelease(p);}
            assertEquals(1028,input[0].getAcquire(1028));
        } finally {r.abort(new RuntimeException("test finished"));worker.get().join(1000);}
    }
    @Test(timeout=10000) public void fullSizeFrameAfterPaddingAndAfterExactEnd() {
        for(var mode:Revelator.PublicationMode.values()) {
            AtomicInteger count=new AtomicInteger();
            try(var r=Revelator.create(1024,ProcessorsFactories.single((b,i,n,t,p,id,k)->count.incrementAndGet()),Thread::new,mode)){
                r.start();
                for(int i=0;i<100;i++) {
                    long p=r.claimSingleMessage(3,0,0,(byte)1);r.publish(p+3);
                    p=r.claimSingleMessage(1004,0,0,(byte)1);r.publish(p+1004);
                }
            }
            assertEquals(200,count.get());
        }
    }
    @Test(timeout=10000) public void headerScannerSupportsCooperativePipeline() {
        AtomicInteger count=new AtomicInteger();
        try(var r=Revelator.create(1024,(f,c)->{
            PipelinedStageHandler<PipelinedFlowSession> h=s->{count.incrementAndGet();return true;};
            var p=new PipelinedFlowProcessor<>(List.of(h),PipelinedFlowSession::new,f,c.getIndexMask(),c.getBuffer());
            return new IFlowProcessorsFactory.ProcessorsChain(List.of(p),p.getReleasingFence());
        },Thread::new,Revelator.PublicationMode.HEADER_COMMIT)){
            r.start();for(int i=0;i<500;i++){long p=r.claimSingleMessage(17,0,0,(byte)1);r.publish(p+17);}
        }
        assertEquals(500,count.get());
    }
}
