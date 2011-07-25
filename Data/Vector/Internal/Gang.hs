{-# LANGUAGE GADTs #-}
-- |
-- Gang primitives based on Repa code by Ben Lippmeier, in turn based on DPH
-- code by Roman Leschinskiy.
--

module Data.Vector.Internal.Gang (

  Gang, Work, theGang, gangSize, gangIO, gangST

) where

import Control.Monad
import Control.Monad.ST
import Control.Exception
import Control.Concurrent.MVar
import System.IO.Unsafe
import GHC.Conc                         ( forkOnIO, numCapabilities )

import Debug.Trace


-- A 'Gang' is a group of threads which execute arbitrary work requests. To get
-- the gang to do work, write Req-uest values to its RVars.
--
data Gang where
  Gang :: Int -> [RVar Req] -> RVar Busy -> Gang

instance Show Gang where
  showsPrec p (Gang n _ _) = showString "<<"
                           . showsPrec p n
                           . showString " threads>>"

-- The gang is shared by all computations.
--
{-# NOINLINE theGang #-}
theGang :: Gang
theGang = unsafePerformIO $ forkGang numCapabilities

-- Number of threads in the main worker gang.
--
gangSize :: Gang -> Int
gangSize (Gang n _ _) = n

-- Fork a gang with the given number of threads
--
forkGang :: Int -> IO Gang
forkGang n = do
  threads <- mapM spawn [0 .. n-1]
  busy    <- newMVar False
  return $!  Gang n threads busy
  where
    spawn tid = do
      req <- newEmptyMVar
      addMVarFinalizer req $ do stop <- shutdown
                                putMVar req stop
                                wait stop
      void $ forkOnIO tid (worker tid req)
      return req


-- The main loop of a gang worker. Threads block waiting for requests to be sent
-- through their RVars.
--
worker :: Int -> RVar Req -> IO ()
worker tid reqVar = loop
  where loop = do
          req <- takeMVar reqVar
          case req of
            Stop done         -> putMVar done (Right ())
            New result action -> do
              putMVar result =<< try (action tid)
              loop


-- Issue work requests for the gang and wait until they have executed. If the
-- gang is already busy, then just run sequentially in the requesting thread.
--
{-# NOINLINE gangIO #-}
gangIO :: Gang -> Work -> IO ()
gangIO (Gang n workers busy) action = do
  active <- swapMVar busy True
  if active then seqIO
            else parIO
  where
    seqIO = trace "gang is busy: running sequentially"
          $ mapM_ action [0 .. n-1]

    parIO = do
      requests <- replicateM n (new action)
      (zipWithM_ putMVar workers requests >> mapM_ wait requests)
        `finally`
        swapMVar busy False

-- Same as gangIO, but in the ST monad
--
{-# NOINLINE gangST #-}
gangST :: Gang -> (Int -> ST s ()) -> ST s ()
gangST gang action = unsafeIOToST . gangIO gang $ unsafeSTToIO . action


-- Encapsulation for work requests sent to individual threads
--
type RVar a = MVar a
type Done   = Either SomeException ()
type Busy   = Bool
type Work   = Int -> IO ()


-- Do some work, then signal completion by writing to the mvar
--
data Req where
  New   :: RVar Done -> Work -> Req
  Stop  :: RVar Done         -> Req

new :: Work -> IO Req
new action = flip New action `fmap` newEmptyMVar

shutdown :: IO Req
shutdown = Stop `fmap` newEmptyMVar

wait :: Req -> IO ()
wait req = do
  ans <- case req of
    New  res  _ -> takeMVar res
    Stop done   -> takeMVar done
  --
  either throwIO return ans

